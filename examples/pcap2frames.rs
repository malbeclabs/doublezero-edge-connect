//! pcap -> edge-feed frame-log converter (dev tooling).
//!
//! Reads a multicast pcap, selects ONE publisher by **source IP** (the demux key the bridge keys
//! on — robust to publishers sharing a UDP port in future, per the feed team), and emits the
//! harness's `[u32 LE length][frame bytes]` record format that `tests/common/replay.rs`
//! (`split_frames`) replays. One UDP datagram == one edge-feed frame, so a "frame" is the UDP
//! payload.
//!
//! Frames are decoded through the real `ingest::codec` / `ingest::codec_mbo`, which does double
//! duty:
//!   1. **Filtering** — with `--symbol BTC`, keep every refdata frame (definitions + manifest,
//!      needed so the bridge resolves precision) but only the mktdata (and, for MBO, snapshot)
//!      frames for that symbol, yielding a small self-contained fixture.
//!   2. **Validation** — decode failures and message-type tallies are reported, so running this
//!      against a live capture validates the codec's byte offsets against the real feed. (This is
//!      the first real-feed check of the MBO offsets, which are otherwise only self-consistent.)
//!
//! Output is split by content into per-role files matching the harness's separate replay streams:
//!   TOB -> `<prefix>.refdata.bin`, `<prefix>.mktdata.bin`
//!   MBO -> `<prefix>.refdata.bin`, `<prefix>.snapshot.bin`, `<prefix>.mktdata.bin`
//!
//! The capture is Linux SLL (cooked) — we hand-parse SLL(16) -> IPv4 -> UDP, which is trivial for
//! the multicast UDP we care about and avoids an Ethernet-only parser.

use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{BufWriter, Write},
    net::Ipv4Addr,
    path::{Path, PathBuf},
};

use anyhow::{anyhow, bail, Context, Result};
use clap::{Parser, ValueEnum};
use pcap_file::{pcap::PcapReader, DataLink};

use doublezero_edge_connect::ingest::{codec, codec_mbo};

/// One replay stream: a list of complete frames (UDP payloads), each written as a length-prefixed
/// record by [`write_log`].
type FrameLog = Vec<Vec<u8>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Protocol {
    Tob,
    Mbo,
}

impl Protocol {
    /// Little-endian frame magic: TOB `0x445A`, MBO `0x4444`.
    fn magic(self) -> [u8; 2] {
        match self {
            Protocol::Tob => [0x5A, 0x44],
            Protocol::Mbo => [0x44, 0x44],
        }
    }
}

#[derive(Parser, Debug)]
#[command(about = "Convert a multicast pcap into per-publisher edge-feed frame-logs")]
struct Args {
    /// Input pcap (Linux SLL / cooked capture).
    pcap: PathBuf,
    /// Output prefix; per-role suffixes are appended (e.g. `<prefix>.mktdata.bin`).
    #[arg(short, long)]
    out: PathBuf,
    /// Publisher source IP to select (the demux key).
    #[arg(long)]
    src: Ipv4Addr,
    /// Multicast group (destination) to filter on.
    #[arg(long, default_value = "233.84.178.15")]
    group: Ipv4Addr,
    /// Which protocol's frames to extract.
    #[arg(long, value_enum, default_value = "tob")]
    protocol: Protocol,
    /// Keep only mktdata/snapshot frames for these symbols (e.g. `--symbol BTC --symbol ETH`).
    /// Repeatable. Omit to keep all symbols. A frame batches several instruments, so a frame
    /// carrying any selected symbol is kept whole (it may also carry unselected ones).
    #[arg(long)]
    symbol: Vec<String>,
    /// Window start (seconds, relative to the first packet in the capture).
    #[arg(long, default_value_t = 0.0)]
    from: f64,
    /// Window end (seconds, relative to the first packet). Omit for end-of-capture.
    #[arg(long)]
    to: Option<f64>,
    /// MBO only: trim to a minimal two-sided fixture — the first COMPLETE snapshot group
    /// (received `SnapshotOrder`s == the begin's promised `total_orders`) for the selected symbol,
    /// plus the contiguous post-anchor deltas (capped by `--mbo-max-deltas`). A live deep-book MBO
    /// snapshot is tens of thousands of orders; this keeps a real two-sided book without committing
    /// the whole multi-MB window. Requires exactly one `--symbol`.
    #[arg(long)]
    mbo_minimal: bool,
    /// Cap on post-anchor deltas kept under `--mbo-minimal`.
    #[arg(long, default_value_t = 300)]
    mbo_max_deltas: u32,
    /// Second publisher source IP. When set, emit ONE combined `<out>.combined.bin` of both
    /// publishers' refdata + symbol-filtered TOB mktdata **in capture order**, each record tagged
    /// `[u32 len][4B src_ip][1B role: 0=refdata, 1=mktdata][frame]`. This preserves the real
    /// inter-publisher interleaving the multi-publisher dedup must collapse (separate per-publisher
    /// files, replayed back-to-back, would not). TOB only.
    #[arg(long)]
    combined_with: Option<Ipv4Addr>,
}

/// Extract the UDP payload of an SLL-encapsulated IPv4/UDP datagram, plus its src and dst IP.
/// Returns None for anything that isn't IPv4/UDP or is too short.
fn sll_ipv4_udp(data: &[u8]) -> Option<(Ipv4Addr, Ipv4Addr, &[u8])> {
    if data.len() < 16 + 20 + 8 {
        return None;
    }
    // SLL v1: 16-byte header; bytes [14..16] are the protocol (EtherType, big-endian).
    if u16::from_be_bytes([data[14], data[15]]) != 0x0800 {
        return None; // not IPv4
    }
    let ip = &data[16..];
    if (ip[0] >> 4) != 4 {
        return None;
    }
    let ihl = ((ip[0] & 0x0f) as usize) * 4;
    if ihl < 20 || ip.len() < ihl + 8 || ip[9] != 17 {
        return None; // not UDP
    }
    let src = Ipv4Addr::new(ip[12], ip[13], ip[14], ip[15]);
    let dst = Ipv4Addr::new(ip[16], ip[17], ip[18], ip[19]);
    let udp = &ip[ihl..];
    let udp_len = u16::from_be_bytes([udp[4], udp[5]]) as usize;
    // UDP length covers header(8)+payload; clamp to available bytes defensively.
    let end = udp_len.clamp(8, udp.len());
    Some((src, dst, &udp[8..end]))
}

/// Read the pcap and return this publisher's frames for the chosen protocol, in capture order.
fn collect_frames(args: &Args) -> Result<Vec<Vec<u8>>> {
    let file = File::open(&args.pcap).with_context(|| format!("open {:?}", args.pcap))?;
    let mut reader = PcapReader::new(file).map_err(|e| anyhow!("read pcap header: {e}"))?;
    // Only Linux SLL v1 (DLT 113) is parsed below; bail loudly on anything else. A plain-Ethernet
    // (DLT 1) or SLL2 (DLT 276) capture would otherwise parse zero matching frames and exit with a
    // misleading "matched 0 frames" success — the silent bad-input trap.
    let datalink = reader.header().datalink;
    if datalink != DataLink::LINUX_SLL {
        bail!("unsupported pcap link type {datalink:?}; this tool parses Linux SLL (DLT 113) only");
    }
    let magic = args.protocol.magic();

    let mut frames = Vec::new();
    // `first_ts` anchors to the FIRST packet in the file (not the first in-window packet), so
    // `--from`/`--to` are relative to capture start.
    let mut first_ts: Option<f64> = None;
    while let Some(pkt) = reader.next_packet() {
        let pkt = pkt.map_err(|e| anyhow!("read packet: {e}"))?;
        let rel =
            pkt.timestamp.as_secs_f64() - *first_ts.get_or_insert(pkt.timestamp.as_secs_f64());
        if rel < args.from {
            continue;
        }
        if args.to.is_some_and(|to| rel > to) {
            break; // pcap is time-ordered; past the window
        }
        let Some((src, dst, payload)) = sll_ipv4_udp(&pkt.data) else {
            continue;
        };
        // A publisher emits both TOB and MBO on different ports for the same (src, group); the
        // magic check keeps only the requested protocol so the other doesn't look like a decode
        // failure.
        if src == args.src && dst == args.group && payload.len() >= 24 && payload[..2] == magic {
            frames.push(payload.to_vec());
        }
    }
    Ok(frames)
}

fn write_log(path: &Path, frames: &[Vec<u8>]) -> Result<()> {
    let mut w = BufWriter::new(File::create(path).with_context(|| format!("create {path:?}"))?);
    for f in frames {
        w.write_all(&(f.len() as u32).to_le_bytes())?;
        w.write_all(f)?;
    }
    w.flush()?;
    Ok(())
}

fn out_path(prefix: &Path, role: &str) -> PathBuf {
    prefix.with_extension(format!("{role}.bin"))
}

/// Resolve `--symbol` selections to their instrument ids from the discovered definitions.
/// Returns `None` when no symbols were given (keep all), or `Some(set)` of the resolved ids.
/// Errors if any requested symbol has no definition in the window.
fn resolve_symbols(symbols: &[String], map: &HashMap<String, u32>) -> Result<Option<HashSet<u32>>> {
    if symbols.is_empty() {
        return Ok(None);
    }
    let mut ids = HashSet::new();
    for sym in symbols {
        match map.get(sym) {
            Some(&id) => {
                ids.insert(id);
            }
            None => bail!(
                "symbol {sym:?} not found among {} definitions in the window (known: {:?})",
                map.len(),
                map.keys().collect::<Vec<_>>()
            ),
        }
    }
    Ok(Some(ids))
}

fn report_decode(label: &str, args: &Args, defs: usize, errors: u64, body: &str) {
    eprintln!("publisher {} on group {} [{label}]:", args.src, args.group);
    eprintln!("  {body}");
    eprintln!("  symbols defined: {defs}");
    if errors > 0 {
        eprintln!(
            "  WARNING: {errors} frames failed to decode — possible codec offset mismatch vs the live feed"
        );
    }
}

/// TOB: split into refdata (definitions/manifest) and mktdata (quotes/trades, symbol-filtered).
fn process_tob(frames: &[Vec<u8>], args: &Args) -> Result<()> {
    use codec::Message;
    let (mut quotes, mut trades, mut defs, mut manifests, mut hb, mut other, mut errors) =
        (0u64, 0u64, 0u64, 0u64, 0u64, 0u64, 0u64);
    let mut symbol_to_id: HashMap<String, u32> = HashMap::new();
    let mut refdata: Vec<Vec<u8>> = Vec::new();
    // Buffered with their instrument ids: the symbol filter is resolved after the full scan,
    // since a definition may arrive after early quotes.
    let mut mkt: Vec<(Vec<u8>, Vec<u32>)> = Vec::new();

    for f in frames {
        let Ok((_hdr, msgs)) = codec::decode_frame(f) else {
            errors += 1;
            continue;
        };
        let mut has_refdata = false;
        let mut ids = Vec::new();
        for m in &msgs {
            match m {
                Message::Quote(q) => {
                    quotes += 1;
                    ids.push(q.instrument_id);
                }
                Message::Trade(t) => {
                    trades += 1;
                    ids.push(t.instrument_id);
                }
                Message::InstrumentDefinition(d) => {
                    defs += 1;
                    has_refdata = true;
                    symbol_to_id.insert(d.symbol.clone(), d.instrument_id);
                }
                Message::ManifestSummary(_) => {
                    manifests += 1;
                    has_refdata = true;
                }
                Message::Heartbeat => hb += 1,
                _ => other += 1,
            }
        }
        if has_refdata {
            refdata.push(f.clone());
        }
        if !ids.is_empty() {
            mkt.push((f.clone(), ids));
        }
    }

    let target = resolve_symbols(&args.symbol, &symbol_to_id)?;
    // A TOB frame batches multiple instruments' messages, so a frame carrying a selected symbol may
    // also carry others; it is kept whole. Track that leakage so the output is not overclaimed as
    // "selected-only".
    let mut mktdata: Vec<Vec<u8>> = Vec::new();
    let (mut mixed_frames, mut leaked_msgs) = (0u64, 0u64);
    for (f, ids) in mkt {
        let keep = target
            .as_ref()
            .is_none_or(|t| ids.iter().any(|id| t.contains(id)));
        if keep {
            if let Some(t) = &target {
                let others = ids.iter().filter(|id| !t.contains(id)).count() as u64;
                if others > 0 {
                    mixed_frames += 1;
                    leaked_msgs += others;
                }
            }
            mktdata.push(f);
        }
    }

    let refdata_path = out_path(&args.out, "refdata");
    let mktdata_path = out_path(&args.out, "mktdata");
    write_log(&refdata_path, &refdata)?;
    write_log(&mktdata_path, &mktdata)?;
    report_decode(
        "tob",
        args,
        symbol_to_id.len(),
        errors,
        &format!("decode: quotes={quotes} trades={trades} defs={defs} manifests={manifests} heartbeats={hb} other={other} errors={errors}"),
    );
    if !args.symbol.is_empty() {
        eprintln!(
            "  filter symbols {:?} -> instrument_ids {target:?}",
            args.symbol
        );
        if mixed_frames > 0 {
            eprintln!(
                "  note: {mixed_frames} kept frames also batch unselected symbols ({leaked_msgs} non-selected messages retained — frames are kept whole)"
            );
        }
    }
    eprintln!(
        "  wrote {} refdata frames -> {refdata_path:?}",
        refdata.len()
    );
    eprintln!(
        "  wrote {} mktdata frames -> {mktdata_path:?}",
        mktdata.len()
    );
    Ok(())
}

/// MBO: split into refdata (definitions/manifest), snapshot (begin/order/end), and mktdata (order
/// deltas + trades). Symbol filter keeps that instrument's deltas/trades and its snapshot group
/// (SnapshotOrder carries only a snapshot_id, so we keep the orders whose snapshot_id belongs to a
/// SnapshotBegin for the target instrument).
fn process_mbo(frames: &[Vec<u8>], args: &Args) -> Result<()> {
    use codec_mbo::Message;
    let (mut adds, mut cancels, mut execs, mut trades) = (0u64, 0u64, 0u64, 0u64);
    let (mut defs, mut manifests, mut snaps, mut hb, mut other, mut errors) =
        (0u64, 0u64, 0u64, 0u64, 0u64, 0u64);
    let mut symbol_to_id: HashMap<String, u32> = HashMap::new();

    // Per-frame summaries (so the symbol filter can be applied after the full scan).
    struct Frame {
        payload: Vec<u8>,
        refdata: bool,
        snapshot: bool,
        begins: Vec<(u32, u32)>, // (instrument_id, snapshot_id)
        end_insts: Vec<u32>,
        order_sids: Vec<u32>,
        md_ids: Vec<u32>, // delta/trade instrument ids
    }
    let mut summaries: Vec<Frame> = Vec::new();

    for f in frames {
        let Ok((_hdr, msgs)) = codec_mbo::decode_frame(f) else {
            errors += 1;
            continue;
        };
        let mut fr = Frame {
            payload: f.clone(),
            refdata: false,
            snapshot: false,
            begins: Vec::new(),
            end_insts: Vec::new(),
            order_sids: Vec::new(),
            md_ids: Vec::new(),
        };
        for m in &msgs {
            match m {
                Message::OrderAdd(o) => {
                    adds += 1;
                    fr.md_ids.push(o.instrument_id);
                }
                Message::OrderCancel(o) => {
                    cancels += 1;
                    fr.md_ids.push(o.instrument_id);
                }
                Message::OrderExecute(o) => {
                    execs += 1;
                    fr.md_ids.push(o.instrument_id);
                }
                Message::Trade(t) => {
                    trades += 1;
                    fr.md_ids.push(t.instrument_id);
                }
                Message::InstrumentDefinition(d) => {
                    defs += 1;
                    fr.refdata = true;
                    symbol_to_id.insert(d.symbol.clone(), d.instrument_id);
                }
                Message::ManifestSummary(_) => {
                    manifests += 1;
                    fr.refdata = true;
                }
                Message::SnapshotBegin(s) => {
                    snaps += 1;
                    fr.snapshot = true;
                    fr.begins.push((s.instrument_id, s.snapshot_id));
                }
                Message::SnapshotOrder(s) => {
                    snaps += 1;
                    fr.snapshot = true;
                    fr.order_sids.push(s.snapshot_id);
                }
                Message::SnapshotEnd(s) => {
                    snaps += 1;
                    fr.snapshot = true;
                    fr.end_insts.push(s.instrument_id);
                }
                Message::Heartbeat => hb += 1,
                _ => other += 1,
            }
        }
        summaries.push(fr);
    }

    let target = resolve_symbols(&args.symbol, &symbol_to_id)?;
    // Snapshot ids belonging to the selected instruments' snapshot groups.
    let target_sids: HashSet<u32> = match &target {
        Some(t) => summaries
            .iter()
            .flat_map(|fr| fr.begins.iter())
            .filter(|(inst, _)| t.contains(inst))
            .map(|(_, sid)| *sid)
            .collect(),
        None => HashSet::new(),
    };

    let mut refdata = Vec::new();
    let mut snapshot = Vec::new();
    let mut mktdata = Vec::new();
    for fr in &summaries {
        if fr.refdata {
            refdata.push(fr.payload.clone());
        }
        if fr.snapshot {
            let keep = match &target {
                None => true,
                Some(t) => {
                    fr.begins.iter().any(|(inst, _)| t.contains(inst))
                        || fr.end_insts.iter().any(|inst| t.contains(inst))
                        || fr.order_sids.iter().any(|sid| target_sids.contains(sid))
                }
            };
            if keep {
                snapshot.push(fr.payload.clone());
            }
        }
        let keep_md = target
            .as_ref()
            .is_none_or(|t| fr.md_ids.iter().any(|id| t.contains(id)));
        if !fr.md_ids.is_empty() && keep_md {
            mktdata.push(fr.payload.clone());
        }
    }

    if args.mbo_minimal {
        let target_id = match &target {
            Some(t) if t.len() == 1 => *t.iter().next().unwrap(),
            _ => bail!("--mbo-minimal requires exactly one --symbol"),
        };
        (refdata, snapshot, mktdata) = trim_mbo_minimal(
            &refdata,
            &snapshot,
            &mktdata,
            target_id,
            args.mbo_max_deltas,
        )?;
    }

    let refdata_path = out_path(&args.out, "refdata");
    let snapshot_path = out_path(&args.out, "snapshot");
    let mktdata_path = out_path(&args.out, "mktdata");
    write_log(&refdata_path, &refdata)?;
    write_log(&snapshot_path, &snapshot)?;
    write_log(&mktdata_path, &mktdata)?;
    report_decode(
        "mbo",
        args,
        symbol_to_id.len(),
        errors,
        &format!("decode: order_add={adds} order_cancel={cancels} order_execute={execs} trades={trades} defs={defs} manifests={manifests} snapshot_msgs={snaps} heartbeats={hb} other={other} errors={errors}"),
    );
    if !args.symbol.is_empty() {
        eprintln!(
            "  filter symbols {:?} -> instrument_ids {target:?} (snapshot ids {target_sids:?})",
            args.symbol
        );
    }
    eprintln!(
        "  wrote {} refdata frames -> {refdata_path:?}",
        refdata.len()
    );
    eprintln!(
        "  wrote {} snapshot frames -> {snapshot_path:?}",
        snapshot.len()
    );
    eprintln!(
        "  wrote {} mktdata frames -> {mktdata_path:?}",
        mktdata.len()
    );
    Ok(())
}

/// Index of the first frame in `frames` whose decoded messages satisfy `pred`, or `Ok(None)` if
/// none match. Unlike a `position()` closure - which would have to `.unwrap()` the decode and panic
/// on a malformed frame - this propagates a decode error via `?`, matching the rest of the
/// generator's fail-loud handling.
fn first_frame_with(
    frames: &[Vec<u8>],
    pred: impl Fn(&codec_mbo::Message) -> bool,
) -> Result<Option<usize>> {
    for (i, f) in frames.iter().enumerate() {
        if codec_mbo::decode_frame(f)?.1.iter().any(&pred) {
            return Ok(Some(i));
        }
    }
    Ok(None)
}

/// Trim already-symbol-filtered MBO refdata+snapshot+mktdata vectors to a minimal two-sided
/// fixture: one manifest + the target definition (enough to resolve precision), the first COMPLETE
/// snapshot group for `target_id` (every promised order present, with a matching `SnapshotEnd`),
/// plus the contiguous post-anchor deltas (per-instrument seq in
/// `(last_instrument_seq, last_instrument_seq + max_deltas]`). Decoding through `codec_mbo` doubles
/// as validation: an incomplete capture (no complete group) fails loudly here, and the bid/ask
/// split is reported so the fixture's two-sidedness is confirmed at generation time.
fn trim_mbo_minimal(
    refdata: &[Vec<u8>],
    snapshot: &[Vec<u8>],
    mktdata: &[Vec<u8>],
    target_id: u32,
    max_deltas: u32,
) -> Result<(FrameLog, FrameLog, FrameLog)> {
    use codec_mbo::Message;

    // Refdata: keep from the first ManifestSummary frame through the first frame at/after it that
    // carries the target instrument's definition. One manifest epoch + the one definition is all the
    // subscriber needs to resolve `target_id`'s precision; the live capture re-sends the same
    // manifest seq on a round-robin, so the rest is redundant.
    let manifest_idx = first_frame_with(refdata, |m| matches!(m, Message::ManifestSummary(_)))?
        .ok_or_else(|| anyhow!("no ManifestSummary in refdata"))?;
    let def_off = first_frame_with(
        &refdata[manifest_idx..],
        |m| matches!(m, Message::InstrumentDefinition(d) if d.instrument_id == target_id),
    )?
    .ok_or_else(|| {
        anyhow!("no definition for instrument {target_id} at/after the first manifest")
    })?;
    let refdata_out = refdata[manifest_idx..=manifest_idx + def_off].to_vec();

    // Pass 1: tally each snapshot group (in begin order) - promised total, received order count,
    // and whether it ended - so the selection below can pick the first complete one.
    let mut begins: Vec<(u32, u32, u32, u32)> = Vec::new(); // (sid, total, instrument_id, last_instr_seq)
    let mut received: HashMap<u32, u32> = HashMap::new();
    let mut ended: HashSet<u32> = HashSet::new();
    for f in snapshot {
        for m in &codec_mbo::decode_frame(f)?.1 {
            match m {
                Message::SnapshotBegin(s) => begins.push((
                    s.snapshot_id,
                    s.total_orders,
                    s.instrument_id,
                    s.last_instrument_seq,
                )),
                Message::SnapshotOrder(s) => *received.entry(s.snapshot_id).or_default() += 1,
                Message::SnapshotEnd(s) => {
                    ended.insert(s.snapshot_id);
                }
                _ => {}
            }
        }
    }
    // Pick the first complete group for target_id (every promised order present). A group whose
    // SnapshotOrder count doesn't match its begin's promised total is lossy or replayed - book.rs
    // rejects it on SnapshotEnd (`received != total`), so it can't seed the fixture; warn to surface
    // the malformed capture rather than silently passing it over, then keep scanning.
    let mut chosen = None;
    for &(sid, total, inst, last_instr_seq) in &begins {
        if inst != target_id || !ended.contains(&sid) {
            continue;
        }
        let got = received.get(&sid).copied().unwrap_or(0);
        if got != total {
            eprintln!(
                "  mbo-minimal: WARNING snapshot group sid={sid} (instrument {target_id}) carried \
                 {got} SnapshotOrders but its begin promised {total} (lossy or replayed capture); skipping"
            );
            continue;
        }
        chosen = Some((sid, total, last_instr_seq));
        break;
    }
    let (sid, total, last_instr_seq) = chosen.ok_or_else(|| {
        anyhow!(
            "no complete snapshot group for instrument {target_id} (capture lost packets in every group); \
             groups seen: {begins:?}, received: {received:?}"
        )
    })?;

    // Pass 2: keep the chosen group's frames [BEGIN..END] inclusive, and count its two sides.
    let begin_idx = first_frame_with(
        snapshot,
        |m| matches!(m, Message::SnapshotBegin(s) if s.snapshot_id == sid),
    )?
    .expect("chosen snapshot group has a begin frame");
    let end_idx = first_frame_with(
        snapshot,
        |m| matches!(m, Message::SnapshotEnd(s) if s.snapshot_id == sid),
    )?
    .expect("chosen snapshot group has an end frame");
    let snap_out = snapshot[begin_idx..=end_idx].to_vec();
    let (mut bid, mut ask) = (0u32, 0u32);
    for f in &snap_out {
        for m in &codec_mbo::decode_frame(f)?.1 {
            if let Message::SnapshotOrder(s) = m {
                if s.snapshot_id == sid {
                    if s.side == codec_mbo::SIDE_BID {
                        bid += 1
                    } else {
                        ask += 1
                    }
                }
            }
        }
    }

    // Post-anchor deltas: contiguous per-instrument seqs after the snapshot's last_instrument_seq.
    let (lo, hi) = (
        last_instr_seq + 1,
        last_instr_seq.saturating_add(max_deltas),
    );
    let mut md_out = Vec::new();
    let (mut kmin, mut kmax) = (u32::MAX, 0u32);
    for f in mktdata {
        let mut keep = false;
        for m in &codec_mbo::decode_frame(f)?.1 {
            let seq = match m {
                Message::OrderAdd(o) if o.instrument_id == target_id => o.per_instrument_seq,
                Message::OrderCancel(o) if o.instrument_id == target_id => o.per_instrument_seq,
                Message::OrderExecute(o) if o.instrument_id == target_id => o.per_instrument_seq,
                _ => continue,
            };
            if (lo..=hi).contains(&seq) {
                keep = true;
                kmin = kmin.min(seq);
                kmax = kmax.max(seq);
            }
        }
        if keep {
            md_out.push(f.clone());
        }
    }

    eprintln!(
        "  mbo-minimal: {} refdata frames; snapshot group sid={sid} total_orders={total} \
         (bid={bid} ask={ask}, two-sided), {} snapshot frames; {} mktdata frames, \
         post-anchor seq=[{kmin}..{kmax}]",
        refdata_out.len(),
        snap_out.len(),
        md_out.len()
    );
    Ok((refdata_out, snap_out, md_out))
}

/// Collect `(src_ip, frame)` for any of `srcs`, in capture order — like `collect_frames` but tags
/// the source and accepts a set, for the combined multi-publisher output.
fn collect_tagged(args: &Args, srcs: &[Ipv4Addr]) -> Result<Vec<(Ipv4Addr, Vec<u8>)>> {
    let file = File::open(&args.pcap).with_context(|| format!("open {:?}", args.pcap))?;
    let mut reader = PcapReader::new(file).map_err(|e| anyhow!("read pcap header: {e}"))?;
    let datalink = reader.header().datalink;
    if datalink != DataLink::LINUX_SLL {
        bail!("unsupported pcap link type {datalink:?}; this tool parses Linux SLL (DLT 113) only");
    }
    let magic = args.protocol.magic();
    let mut out = Vec::new();
    let mut first_ts: Option<f64> = None;
    while let Some(pkt) = reader.next_packet() {
        let pkt = pkt.map_err(|e| anyhow!("read packet: {e}"))?;
        let rel =
            pkt.timestamp.as_secs_f64() - *first_ts.get_or_insert(pkt.timestamp.as_secs_f64());
        if rel < args.from {
            continue;
        }
        if args.to.is_some_and(|to| rel > to) {
            break;
        }
        let Some((src, dst, payload)) = sll_ipv4_udp(&pkt.data) else {
            continue;
        };
        if srcs.contains(&src) && dst == args.group && payload.len() >= 24 && payload[..2] == magic
        {
            out.push((src, payload.to_vec()));
        }
    }
    Ok(out)
}

/// Combined multi-publisher TOB output: both publishers' refdata + symbol-filtered mktdata in
/// capture order, each record tagged with its source IP and role, preserving the real interleaving.
fn process_tob_combined(tagged: &[(Ipv4Addr, Vec<u8>)], args: &Args) -> Result<()> {
    use codec::Message;
    // Pass 1: build symbol -> id across both publishers (a def may follow early quotes).
    let mut symbol_to_id: HashMap<String, u32> = HashMap::new();
    for (_src, f) in tagged {
        if let Ok((_h, msgs)) = codec::decode_frame(f) {
            for m in &msgs {
                if let Message::InstrumentDefinition(d) = m {
                    symbol_to_id.insert(d.symbol.clone(), d.instrument_id);
                }
            }
        }
    }
    let target = resolve_symbols(&args.symbol, &symbol_to_id)?;
    // id -> symbol, for the per-symbol count report (helps pick busy vs quiet coins).
    let id_to_symbol: HashMap<u32, String> = symbol_to_id
        .iter()
        .map(|(s, &id)| (id, s.clone()))
        .collect();

    // Pass 2: classify each frame (refdata vs selected mktdata) and emit in capture order, tagged.
    let path = out_path(&args.out, "combined");
    let mut w = BufWriter::new(File::create(&path).with_context(|| format!("create {path:?}"))?);
    let (mut refc, mut mktc, mut errors) = (0u64, 0u64, 0u64);
    // Raw per-(symbol, publisher) quote-message counts among the KEPT mktdata frames — the
    // pre-dedup baseline the dedup test compares its emitted counts against.
    let mut per_symbol_pub: HashMap<(String, Ipv4Addr), u64> = HashMap::new();
    for (src, f) in tagged {
        let Ok((_h, msgs)) = codec::decode_frame(f) else {
            errors += 1;
            continue;
        };
        let mut is_ref = false;
        let mut md_ids: Vec<u32> = Vec::new();
        for m in &msgs {
            match m {
                Message::InstrumentDefinition(_) | Message::ManifestSummary(_) => is_ref = true,
                Message::Quote(q) => md_ids.push(q.instrument_id),
                Message::Trade(t) => md_ids.push(t.instrument_id),
                _ => {}
            }
        }
        // Keep all refdata (so precision resolves), and mktdata only for the selected symbols.
        let keep_md = !md_ids.is_empty()
            && target
                .as_ref()
                .is_none_or(|t| md_ids.iter().any(|id| t.contains(id)));
        let role: u8 = if is_ref {
            0
        } else if keep_md {
            1
        } else {
            continue;
        };
        if role == 0 {
            refc += 1;
        } else {
            mktc += 1;
            for id in &md_ids {
                if target.as_ref().is_none_or(|t| t.contains(id)) {
                    if let Some(sym) = id_to_symbol.get(id) {
                        *per_symbol_pub.entry((sym.clone(), *src)).or_default() += 1;
                    }
                }
            }
        }
        w.write_all(&(f.len() as u32).to_le_bytes())?;
        w.write_all(&src.octets())?;
        w.write_all(&[role])?;
        w.write_all(f)?;
    }
    w.flush()?;
    eprintln!(
        "combined {} + {}: {refc} refdata + {mktc} mktdata frames (decode errors {errors}) -> {path:?}",
        args.src,
        args.combined_with.expect("combined mode")
    );
    let mut rows: Vec<_> = per_symbol_pub.into_iter().collect();
    rows.sort_by_key(|b| std::cmp::Reverse(b.1));
    eprintln!("  raw kept quote messages per (symbol, publisher):");
    for ((sym, ip), n) in rows {
        eprintln!("    {sym:>8} {ip:>15}  {n}");
    }
    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();
    if let Some(second) = args.combined_with {
        if args.protocol != Protocol::Tob {
            bail!("--combined-with is currently TOB-only");
        }
        let tagged = collect_tagged(&args, &[args.src, second])?;
        eprintln!(
            "matched {} combined TOB frames from {} + {}",
            tagged.len(),
            args.src,
            second
        );
        return process_tob_combined(&tagged, &args);
    }
    let frames = collect_frames(&args)?;
    eprintln!("matched {} {:?} frames", frames.len(), args.protocol);
    match args.protocol {
        Protocol::Tob => process_tob(&frames, &args),
        Protocol::Mbo => process_mbo(&frames, &args),
    }
}
