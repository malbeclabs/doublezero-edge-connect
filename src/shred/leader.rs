//! Slot→leader lookup, backed by a Solana JSON-RPC endpoint and cached per epoch.
//!
//! One `getLeaderSchedule` call returns a whole epoch's schedule (leader pubkey → relative slot
//! indices) in a single response. The cache holds **two** epochs — the current one and the
//! prefetched next — so a slot stays resolvable across a rollover with no gap. `getEpochInfo` gives
//! the current epoch, its first absolute slot, and its length (to locate the next epoch); a
//! background refresher loads whichever of the two isn't cached yet and evicts anything older. The
//! forwarder only ever does a lock-read of the cache on its hot path; all RPC happens off-path in
//! the refresher task.
//!
//! Minimal hand-rolled JSON-RPC over `reqwest` (the issue's "prefer minimal" option) — no
//! `solana-client`.

use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use serde::{de::DeserializeOwned, Deserialize};
use serde_json::{json, Value};
use tokio::sync::RwLock;
use tracing::{info, warn};

/// How often the refresher polls `getEpochInfo` to detect an epoch rollover.
const REFRESH_INTERVAL: Duration = Duration::from_secs(30);
/// Shorter retry after an RPC error, so a transient failure doesn't leave us blind for a full cycle.
const RETRY_INTERVAL: Duration = Duration::from_secs(5);
/// Sanity cap on a leader-schedule slot index. A Solana epoch is 432,000 slots; reject any RPC
/// response whose largest relative index exceeds a generous multiple of that, so a malformed or
/// hostile endpoint can't drive an unbounded allocation in `build_leaders`.
const MAX_SCHEDULE_SLOTS: usize = 4_000_000;

/// The cached leader pubkeys for one epoch, indexed by `slot - first_slot`. `None` marks a slot with
/// no decodable leader (a sparse/garbled schedule), distinct from a real pubkey so the forwarder
/// never verifies against a bogus zero key (it drops that slot instead — see `leader`).
struct EpochLeaders {
    epoch: u64,
    first_slot: u64,
    leaders: Vec<Option<[u8; 32]>>,
}

/// `(epoch, first_slot, leaders)` triple for seeding the cache directly in tests.
#[cfg(test)]
type SeededEpoch = (u64, u64, Vec<Option<[u8; 32]>>);

pub struct LeaderSchedule {
    rpc_url: String,
    client: reqwest::Client,
    /// The current epoch's schedule plus the **prefetched next epoch's**, so a slot is resolvable
    /// across a rollover with no gap. Holds 0..=2 entries; the refresher trims anything older than
    /// the current epoch. A lookup scans both (only ever two).
    inner: RwLock<Vec<EpochLeaders>>,
}

impl LeaderSchedule {
    pub fn new(rpc_url: String) -> Self {
        Self {
            rpc_url,
            client: reqwest::Client::new(),
            inner: RwLock::new(Vec::new()),
        }
    }

    /// Construct a schedule with its cache pre-seeded and no RPC endpoint, for testing the forwarder
    /// without a live RPC. `leaders` is indexed by `slot - first_slot`.
    #[cfg(test)]
    pub(crate) fn with_seeded_cache(
        epoch: u64,
        first_slot: u64,
        leaders: Vec<Option<[u8; 32]>>,
    ) -> Self {
        Self::with_seeded_epochs(vec![(epoch, first_slot, leaders)])
    }

    /// Seed the cache with several epochs at once (current + prefetched next), for testing
    /// cross-epoch lookups without a live RPC. Each tuple is `(epoch, first_slot, leaders)`.
    #[cfg(test)]
    pub(crate) fn with_seeded_epochs(epochs: Vec<SeededEpoch>) -> Self {
        Self {
            rpc_url: String::new(),
            client: reqwest::Client::new(),
            inner: RwLock::new(
                epochs
                    .into_iter()
                    .map(|(epoch, first_slot, leaders)| EpochLeaders {
                        epoch,
                        first_slot,
                        leaders,
                    })
                    .collect(),
            ),
        }
    }

    /// The slot's leader pubkey, or `None` if no cached epoch covers the slot or that slot has no
    /// decodable leader. In sigverify mode `None` makes the forwarder **fail closed** (drop the
    /// shred — it can't be verified). Because the next epoch is prefetched, `None` does not occur at
    /// a routine rollover; it means cold start, a sustained RPC outage that outlived the prefetch
    /// lead, or a garbled schedule.
    pub async fn leader(&self, slot: u64) -> Option<[u8; 32]> {
        let guard = self.inner.read().await;
        guard.iter().find_map(|e| {
            let rel = slot.checked_sub(e.first_slot)? as usize;
            e.leaders.get(rel).copied().flatten()
        })
    }

    /// Ensure the current epoch's schedule is cached and **prefetch the next epoch's**, so a slot
    /// stays resolvable across a rollover with no gap. The current epoch is required (its load error
    /// propagates and is retried); the next-epoch prefetch is best-effort (logged, not fatal) since
    /// it isn't needed until the boundary. Epochs older than the current are evicted. A no-op once
    /// both are cached.
    ///
    /// Fetching each epoch's schedule by an explicit slot (not "current") makes the result
    /// independent of rollover timing, so there's no epoch-boundary race to guard.
    pub async fn refresh(&self) -> Result<()> {
        let info = self.fetch_epoch_info().await?;
        // Untrusted RPC input: a response with slot_index > absolute_slot would underflow.
        let first_slot = info
            .absolute_slot
            .checked_sub(info.slot_index)
            .ok_or_else(|| {
                anyhow!(
                    "getEpochInfo slot_index {} > absolute_slot {}",
                    info.slot_index,
                    info.absolute_slot
                )
            })?;
        let next_first_slot = first_slot
            .checked_add(info.slots_in_epoch)
            .ok_or_else(|| anyhow!("epoch first_slot + slots_in_epoch overflows u64"))?;

        // Drop any schedule for an epoch already behind us.
        self.inner.write().await.retain(|e| e.epoch >= info.epoch);

        self.ensure_epoch(info.epoch, first_slot).await?;
        if let Err(e) = self.ensure_epoch(info.epoch + 1, next_first_slot).await {
            warn!(%e, epoch = info.epoch + 1, "next-epoch leader schedule prefetch failed; will retry");
        }
        Ok(())
    }

    /// Load and cache one epoch's schedule if not already present, indexed from `first_slot`. The
    /// cheap read-lock check skips the fetch in the common already-cached case; the authoritative
    /// re-check happens under the write lock after the fetch, so a duplicate epoch is never pushed
    /// even if two callers raced the read check (today there's only the single refresher task, but
    /// the lock keeps this correct regardless).
    async fn ensure_epoch(&self, epoch: u64, first_slot: u64) -> Result<()> {
        if self.inner.read().await.iter().any(|e| e.epoch == epoch) {
            return Ok(());
        }
        let schedule = self.fetch_leader_schedule(first_slot).await?;
        let leaders = build_leaders(&schedule)?;
        let mut guard = self.inner.write().await;
        if guard.iter().any(|e| e.epoch == epoch) {
            return Ok(()); // lost the race — another fetch already cached this epoch
        }
        info!(
            epoch,
            first_slot,
            slots = leaders.len(),
            "loaded leader schedule"
        );
        guard.push(EpochLeaders {
            epoch,
            first_slot,
            leaders,
        });
        Ok(())
    }

    /// Loop forever refreshing the schedule, polling for epoch rollovers. Never returns (so it is
    /// not the terminal task in the forwarder's `JoinSet`); RPC errors are logged and retried.
    pub async fn run_refresher(&self) {
        loop {
            match self.refresh().await {
                Ok(()) => tokio::time::sleep(REFRESH_INTERVAL).await,
                Err(e) => {
                    warn!(%e, "leader schedule refresh failed; retrying");
                    tokio::time::sleep(RETRY_INTERVAL).await;
                }
            }
        }
    }

    async fn fetch_epoch_info(&self) -> Result<EpochInfo> {
        self.rpc("getEpochInfo", json!([])).await
    }

    async fn fetch_leader_schedule(
        &self,
        slot: u64,
    ) -> Result<std::collections::HashMap<String, Vec<usize>>> {
        // An explicit slot selects the epoch containing it (independent of rollover timing); the
        // default config returns relative slot indices per pubkey.
        self.rpc("getLeaderSchedule", json!([slot])).await
    }

    /// One JSON-RPC call, returning the decoded `result`.
    async fn rpc<T: DeserializeOwned>(&self, method: &str, params: Value) -> Result<T> {
        let body = json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params});
        let resp: RpcResponse<T> = self
            .client
            .post(&self.rpc_url)
            .json(&body)
            .send()
            .await
            .with_context(|| format!("{method} request"))?
            .json()
            .await
            .with_context(|| format!("{method} decode"))?;
        if let Some(err) = resp.error {
            return Err(anyhow!("{method} rpc error {}: {}", err.code, err.message));
        }
        resp.result
            .ok_or_else(|| anyhow!("{method} returned no result"))
    }
}

#[derive(Deserialize)]
struct RpcResponse<T> {
    result: Option<T>,
    error: Option<RpcError>,
}

#[derive(Deserialize)]
struct RpcError {
    code: i64,
    message: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct EpochInfo {
    epoch: u64,
    absolute_slot: u64,
    slot_index: u64,
    slots_in_epoch: u64,
}

/// Turn a `getLeaderSchedule` response (base58 pubkey → relative slot indices) into a vector indexed
/// by relative slot. Every slot in an epoch normally has an assigned leader, so the vector is fully
/// populated; a slot with no decodable leader stays `None` so the forwarder drops that slot (fail
/// closed) rather than verifying against a zero key. The index span is capped
/// ([`MAX_SCHEDULE_SLOTS`]) so a hostile/garbled RPC response can't force an unbounded allocation.
fn build_leaders(
    schedule: &std::collections::HashMap<String, Vec<usize>>,
) -> Result<Vec<Option<[u8; 32]>>> {
    let max = schedule
        .values()
        .flatten()
        .copied()
        .max()
        .ok_or_else(|| anyhow!("empty leader schedule"))?;
    if max >= MAX_SCHEDULE_SLOTS {
        return Err(anyhow!(
            "leader schedule index {max} exceeds sanity cap {MAX_SCHEDULE_SLOTS}"
        ));
    }
    let mut leaders = vec![None; max + 1];
    for (pubkey_b58, indices) in schedule {
        let Some(pubkey) = decode_pubkey(pubkey_b58) else {
            warn!(pubkey = %pubkey_b58, "undecodable leader pubkey; skipping its slots");
            continue;
        };
        for &i in indices {
            if let Some(slot) = leaders.get_mut(i) {
                *slot = Some(pubkey);
            }
        }
    }
    Ok(leaders)
}

fn decode_pubkey(b58: &str) -> Option<[u8; 32]> {
    let bytes = bs58::decode(b58).into_vec().ok()?;
    <[u8; 32]>::try_from(bytes).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn b58(bytes: &[u8; 32]) -> String {
        bs58::encode(bytes).into_string()
    }

    #[test]
    fn build_leaders_is_dense_and_indexed_by_relative_slot() {
        let a = [1u8; 32];
        let b = [2u8; 32];
        let mut sched = HashMap::new();
        sched.insert(b58(&a), vec![0, 2]);
        sched.insert(b58(&b), vec![1, 3]);
        let leaders = build_leaders(&sched).unwrap();
        assert_eq!(leaders, vec![Some(a), Some(b), Some(a), Some(b)]);
    }

    #[test]
    fn build_leaders_leaves_undecodable_slots_none() {
        let a = [3u8; 32];
        let mut sched = HashMap::new();
        sched.insert(b58(&a), vec![1]);
        sched.insert("not-base58-!!!".to_string(), vec![0]);
        let leaders = build_leaders(&sched).unwrap();
        assert_eq!(leaders[1], Some(a));
        assert_eq!(
            leaders[0], None,
            "undecodable pubkey leaves its slot None (never verified against a zero key)"
        );
    }

    #[test]
    fn empty_schedule_is_an_error() {
        assert!(build_leaders(&HashMap::new()).is_err());
    }

    #[test]
    fn build_leaders_rejects_index_past_the_sanity_cap() {
        let a = [4u8; 32];
        let mut sched = HashMap::new();
        sched.insert(b58(&a), vec![MAX_SCHEDULE_SLOTS]);
        assert!(
            build_leaders(&sched).is_err(),
            "an absurd slot index must not drive an unbounded allocation"
        );
    }

    #[test]
    fn seeded_cache_leader_lookup_and_none_for_missing() {
        let pk = [5u8; 32];
        // first_slot = 100; relative slot 5 -> absolute 105 is `pk`, relative 6 is None.
        let leaders = vec![None, None, None, None, None, Some(pk), None];
        let sched = LeaderSchedule::with_seeded_cache(7, 100, leaders);
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        assert_eq!(rt.block_on(sched.leader(105)), Some(pk));
        assert_eq!(rt.block_on(sched.leader(106)), None, "None slot is unknown");
        assert_eq!(rt.block_on(sched.leader(99)), None, "before first_slot");
        assert_eq!(
            rt.block_on(sched.leader(9999)),
            None,
            "past the cached epoch"
        );
    }

    #[test]
    fn leader_resolves_across_current_and_next_epoch() {
        let cur = [1u8; 32];
        let nxt = [2u8; 32];
        // Epoch 10 spans slots 1000..1002; the prefetched epoch 11 spans 1002..1004.
        let sched = LeaderSchedule::with_seeded_epochs(vec![
            (10, 1000, vec![Some(cur), Some(cur)]),
            (11, 1002, vec![Some(nxt), Some(nxt)]),
        ]);
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        assert_eq!(rt.block_on(sched.leader(1000)), Some(cur));
        assert_eq!(rt.block_on(sched.leader(1001)), Some(cur));
        assert_eq!(
            rt.block_on(sched.leader(1002)),
            Some(nxt),
            "a slot in the prefetched next epoch resolves before rollover, no gap"
        );
        assert_eq!(rt.block_on(sched.leader(1003)), Some(nxt));
        assert_eq!(
            rt.block_on(sched.leader(2000)),
            None,
            "a slot outside every cached epoch is unknown"
        );
    }

    #[test]
    fn decode_pubkey_round_trips_and_rejects_wrong_length() {
        let pk = [9u8; 32];
        assert_eq!(decode_pubkey(&b58(&pk)), Some(pk));
        assert_eq!(decode_pubkey(&bs58::encode([0u8; 16]).into_string()), None);
    }
}
