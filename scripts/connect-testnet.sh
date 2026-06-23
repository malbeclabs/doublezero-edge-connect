#!/usr/bin/env bash
#
# DoubleZero Edge Connect installer (testnet)
# -------------------------------------------
# Served from https://get.doublezero.xyz/connect-testnet and run as:
#
#     curl -fsSL https://get.doublezero.xyz/connect-testnet | bash
#
# It checks for Docker (offering to install it), preps the host for GRE, loads the
# access secret, runs the edge-connect bridge container
# (ghcr.io/malbeclabs/doublezero-edge-connect:testnet), runs
# `doublezero connect multicast`, and then serves normalized DZ Edge quotes over a
# WebSocket (:8081). The image bundles doublezerod + the doublezero CLI (it is built
# on the thin client image), so this one container both joins the network and runs
# the bridge.
#
# Attendantless: the only input is the access secret. Provide it via DZ_SECRET to
# run with no prompts at all; otherwise you're prompted once. Everything else has
# a default.
#
# Env vars:
#   DZ_SECRET=<DZ_token|path> base64 keypair token (always prefixed with 'DZ_')
#                             OR a path to a keypair file. If set, runs non-interactively.
#   DZ_ENV=testnet|devnet|mainnet-beta   default: testnet
#   DZ_IMAGE=ghcr.io/malbeclabs/doublezero-edge-connect:testnet
#   DZ_NAME=doublezero-edge-connect      container name
#   DZ_FEEDS=<venue,venue>               optional: narrow ingested venues (default: all)
#   DZ_ASSUME_YES=1                      skip confirmation prompts (e.g. Docker install)
#   DZ_CLIENT_IP=<ipv4>                  override the public IP used by the access-pass pre-check
#   DZ_LEDGER_RPC_URL=<url>              override the DoubleZero ledger RPC used by the pre-check
#
# Any other bridge env var set in the environment is relayed straight to the container
# (WS_*, DZ_IFACE, DZ_SHRED_*, RUST_LOG, ...), so every binary feature can be tuned from the one-liner, e.g.:
#   WS_BIND=0.0.0.0:9000 curl -fsSL https://get.doublezero.xyz/connect-testnet | bash
#
# A DZ_-token-derived keypair is injected straight into the container and is never
# written to the host disk; a keypair supplied as a file path is bind-mounted
# read-only. Either way the plaintext key is not printed.
#
# NOTE: connecting requires the host's public IP to have an access pass / allowlisted
# user onchain for the chosen environment. That provisioning is a separate step; if
# `connect` reports an access-pass error, the rest of the setup is still in place.

set -euo pipefail

# ----------------------------------------------------------------------------
# config / defaults
# ----------------------------------------------------------------------------
DZ_IMAGE="${DZ_IMAGE:-ghcr.io/malbeclabs/doublezero-edge-connect:testnet}"
DZ_NAME="${DZ_NAME:-doublezero-edge-connect}"
DZ_ENV="${DZ_ENV:-testnet}"
DZ_SECRET="${DZ_SECRET:-}"
DZ_FEEDS="${DZ_FEEDS:-}"
DZ_ASSUME_YES="${DZ_ASSUME_YES:-0}"
KEYPAIR_DEST="/root/.config/doublezero/id.json"   # client's default keypair path (container runs as root)
LIVENESS_UDP_PORT=44880
WS_PORT=8081                                       # bridge WebSocket (PROTOCOL.md)
RECV_BUF_MAX=268435456                             # recommended net.core.rmem_max for bursty feeds

# ----------------------------------------------------------------------------
# pretty output + prompts (read from the terminal, not the curl pipe)
# ----------------------------------------------------------------------------
if [ -t 1 ]; then BOLD=$'\033[1m'; RED=$'\033[31m'; YEL=$'\033[33m'; GRN=$'\033[32m'; RST=$'\033[0m'
else BOLD=; RED=; YEL=; GRN=; RST=; fi
info() { printf '%s==>%s %s\n' "$GRN" "$RST" "$*"; }
warn() { printf '%s!! %s%s\n' "$YEL" "$*" "$RST" >&2; }
die()  { printf '%sxx %s%s\n' "$RED" "$*" "$RST" >&2; exit 1; }

# Visual countdown so a freshly-started daemon can finish probing devices before
# we connect. A cold daemon pulls a device list to ping, and `doublezero connect
# multicast` otherwise races that probe and times out. REVISIT: drop this once
# `connect` waits for the daemon itself. Animates on a TTY; under `curl | bash`
# with no terminal it just sleeps.
spin_sleep() {
  secs="$1"; msg="$2"
  if [ -t 1 ]; then
    frames='|/-\'
    for n in $(seq "$secs" -1 1); do
      for k in 1 2 3 4 5; do
        f=$(printf '%s' "$frames" | cut -c "$(( (k - 1) % 4 + 1 ))")
        printf '\r%s==>%s %s %s (%2ss) ' "$GRN" "$RST" "$msg" "$f" "$n"
        sleep 0.2
      done
    done
    printf '\r%s==>%s %s done       \n' "$GRN" "$RST" "$msg"
  else
    info "$msg (waiting ${secs}s)"
    sleep "$secs"
  fi
}

# /dev/tty so prompts work under `curl | bash` (where stdin is the script)
TTY=/dev/tty
ask() {  # ask "Question" "default" -> echoes answer
  local q="$1" def="${2:-}" ans=""
  if [ ! -r "$TTY" ]; then echo "$def"; return; fi
  if [ -n "$def" ]; then printf '%s%s%s [%s]: ' "$BOLD" "$q" "$RST" "$def" >"$TTY"
  else printf '%s%s%s: ' "$BOLD" "$q" "$RST" >"$TTY"; fi
  read -r ans <"$TTY" || true
  echo "${ans:-$def}"
}
confirm() {  # confirm "Question" -> returns 0 if yes
  [ "$DZ_ASSUME_YES" = 1 ] && return 0
  [ -r "$TTY" ] || return 1
  local ans; printf '%s%s%s [y/N]: ' "$BOLD" "$1" "$RST" >"$TTY"
  read -r ans <"$TTY" || true
  case "$ans" in y|Y|yes|YES) return 0;; *) return 1;; esac
}

# ----------------------------------------------------------------------------
# 1. preconditions
# ----------------------------------------------------------------------------
[ "$(uname -s)" = Linux ] || die "This installer supports Linux hosts only (got $(uname -s)). The bridge needs host networking + kernel tunnels."

case "$(uname -m)" in
  x86_64|amd64) : ;;
  *) die "The edge-connect image is published for amd64 only; this host is $(uname -m). Run on an x86_64 Linux box." ;;
esac

# root / sudo: run as a normal user and self-elevate only the privileged steps.
SUDO=""
if [ "$(id -u)" -ne 0 ]; then
  command -v sudo >/dev/null 2>&1 || die "Need root (for Docker + network capabilities) but sudo is not installed. Re-run as root."
  SUDO="sudo"
fi

# Resolve the *human* user's home so the keypair default points at their files
# whether this is invoked as `... | bash` (self-sudo) or `... | sudo bash` (all root).
if [ "$(id -u)" -eq 0 ] && [ -n "${SUDO_USER:-}" ] && [ "$SUDO_USER" != root ]; then
  REAL_HOME="$(getent passwd "$SUDO_USER" 2>/dev/null | cut -d: -f6)"
fi
REAL_HOME="${REAL_HOME:-$HOME}"

# Prime sudo once up front so later privileged commands don't re-prompt mid-run,
# but only ask for a password if one is actually required ('sudo -n true' succeeds
# silently for NOPASSWD or an already-cached timestamp).
if [ -n "$SUDO" ] && ! $SUDO -n true 2>/dev/null; then
  info "Some steps need root; you may be prompted for your password once."
  $SUDO -v || die "Could not obtain sudo. Re-run as root, or configure passwordless sudo."
fi

# ----------------------------------------------------------------------------
# 2. the access secret + onchain access-pass pre-check (before any install)
# ----------------------------------------------------------------------------
# Environment: default testnet, override via DZ_ENV; never prompted.
case "$DZ_ENV" in testnet|devnet|mainnet-beta) : ;; *) die "Invalid DZ_ENV '$DZ_ENV' (testnet|devnet|mainnet-beta)";; esac

# The secret is either a base64 keypair token (always prefixed with 'DZ_') or a
# path to an existing keypair file. Provide via DZ_SECRET (no prompt) or once at
# the prompt.
SECRET="$DZ_SECRET"
if [ -z "$SECRET" ]; then
  SECRET="$(ask 'Access secret (DZ_-prefixed token, or path to a keypair file)' '')"
fi
[ -n "$SECRET" ] || die "No secret provided. Set DZ_SECRET or supply one when prompted."

case "$SECRET" in
  DZ_*)
    # 'DZ_' + base64url(raw 64 keypair bytes). Restore the base64 alphabet and
    # padding, decode to raw bytes, and build the Solana keypair JSON array in
    # memory. It is injected into the container after start and is NEVER written
    # to the host disk.
    b64="$(printf '%s' "${SECRET#DZ_}" | tr '_-' '/+')"
    case $(( ${#b64} % 4 )) in
      2) b64="${b64}==" ;;
      3) b64="${b64}=" ;;
      1) die "Invalid DZ_ token (bad base64url length)." ;;
    esac
    # raw bytes -> single-line "[b1,b2,...]" (collapse od's multi-line output)
    KEY_JSON="[$(printf '%s' "$b64" | base64 -d 2>/dev/null | od -An -v -t u1 | tr -s ' \n' ' ' | sed 's/^ //; s/ $//; s/ /,/g')]"
    n=$(printf '%s' "$KEY_JSON" | tr ',' '\n' | grep -c '[0-9]')
    [ "$n" -eq 64 ] || die "DZ_ token did not decode to a 64-byte keypair (got $n)."
    KEY_SRC=token
    info "Decoded token to a $n-byte keypair (held in memory; not written to host disk)."
    ;;
  *)
    # otherwise it's a path to an existing keypair file (bind-mounted read-only)
    case "$SECRET" in "~"*) SECRET_PATH="${REAL_HOME}${SECRET#\~}";; *) SECRET_PATH="$SECRET";; esac
    [ -f "$SECRET_PATH" ] || die "Secret is not a DZ_-prefixed token, and no keypair file exists at: $SECRET"
    KEYFILE="$(realpath -m "$SECRET_PATH")"
    KEY_SRC=file
    info "Using keypair file: $KEYFILE"
    ;;
esac

# SELinux relabel for the bind mount
MNT_OPT=ro
if command -v getenforce >/dev/null 2>&1 && [ "$(getenforce 2>/dev/null)" = Enforcing ]; then MNT_OPT=ro,Z; fi

# Pre-flight: confirm the identity has an access pass onchain for this host's
# public IP (or 0.0.0.0, the any-IP wildcard) BEFORE we install Docker, pull the
# image, or touch the host network -- so a missing pass fails fast and clearly
# instead of surfacing as a cryptic `doublezero connect` error much later. This
# is pure host-side: derive the identity, compute the access-pass PDA, and read
# it over the ledger's public JSON-RPC. Resolve env -> ledger RPC + serviceability
# program id (DZ_LEDGER_RPC_URL overrides the RPC).
case "$DZ_ENV" in
  mainnet-beta) DZ_RPC_DEF="https://doublezero-mainnet-beta.rpcpool.com/db336024-e7a8-46b1-80e5-352dd77060ab"; DZ_PROG="ser2VaTMAcYTaauMrTSfSrxBaUDq7BLNs2xfUugTAGv";;
  testnet)      DZ_RPC_DEF="https://doublezerolocalnet.rpcpool.com/8a4fd3f4-0977-449f-88c7-63d4b0f10f16"; DZ_PROG="DZtnuQ839pSaDMFG5q1ad2V95G82S5EC4RrB3Ndw2Heb";;
  devnet)       DZ_RPC_DEF="https://doublezerolocalnet.rpcpool.com/8a4fd3f4-0977-449f-88c7-63d4b0f10f16"; DZ_PROG="GYhQDKuESrasNZGyhMJhGYFtbzNijYhcrN9poSqCQVah";;
esac
DZ_RPC="${DZ_LEDGER_RPC_URL:-$DZ_RPC_DEF}"

# Best-effort public IP (DZ_CLIENT_IP overrides; empty if undetectable -> we degrade to a warning).
detect_public_ip() {
  [ -n "${DZ_CLIENT_IP:-}" ] && { printf '%s' "$DZ_CLIENT_IP"; return; }
  local ip
  for url in https://checkip.amazonaws.com https://api.ipify.org https://ifconfig.me/ip; do
    ip="$(curl -fsS -m 3 "$url" 2>/dev/null | tr -d '[:space:]')"
    case "$ip" in *.*.*.*) printf '%s' "$ip"; return;; esac
  done
}

if ! command -v python3 >/dev/null 2>&1; then
  warn "python3 not found; skipping the onchain access-pass pre-check (\`doublezero connect\` will be the fallback)."
else
  info "Verifying the access pass onchain (env=$DZ_ENV)..."
  AP_PUB_IP="$(detect_public_ip)"
  # Feed the 64-byte keypair to python on stdin only (never argv/env), preserving the
  # "plaintext key is never printed or written to host disk" rule; only the public key is shown.
  # The python program is delivered via a /dev/fd process substitution so stdin stays free for the key.
  if [ "$KEY_SRC" = token ]; then AP_FEED="$KEY_JSON"; else AP_FEED="$(cat "$KEYFILE")"; fi
  set +e
  AP_OUT="$(printf '%s' "$AP_FEED" | DZ_RPC="$DZ_RPC" DZ_PROG="$DZ_PROG" PUB_IP="$AP_PUB_IP" DZ_ENV="$DZ_ENV" python3 <(cat <<'PY'
# Host-side access-pass check. stdin: JSON array of the 64 keypair bytes (identity = last 32).
# env: DZ_RPC (ledger JSON-RPC), DZ_PROG (serviceability program id, base58), PUB_IP (this host's
# public IP, "" if unknown). Prints "IDENTITY <b58>" then a verdict line. Exit codes:
#   0 pass found   2 confirmed miss (PUB_IP known; neither it nor 0.0.0.0 has a pass)
#   3 inconclusive (PUB_IP unknown and no 0.0.0.0 pass)   4 inconclusive (RPC/decode error)
# An access pass is the PDA find_program_address([b"doublezero", b"accesspass", client_ip(4),
# identity(32)], program_id); existence (account data[0]==11 == AccountType::AccessPass) == it exists.
import os, sys, json, socket, hashlib, base64, urllib.request
_AB = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
def b58decode(s):
    n = 0
    for c in s:
        n = n * 58 + _AB.index(c)
    full = n.to_bytes((n.bit_length() + 7) // 8, "big") if n else b""
    return b"\x00" * (len(s) - len(s.lstrip("1"))) + full
def b58encode(b):
    n = int.from_bytes(b, "big")
    out = ""
    while n:
        n, r = divmod(n, 58)
        out = _AB[r] + out
    return "1" * (len(b) - len(b.lstrip(b"\x00"))) + out
_P = 2**255 - 19
_D = (-121665 * pow(121666, _P - 2, _P)) % _P
def _on_curve(b):
    if len(b) != 32:
        return False
    y = int.from_bytes(b, "little"); sign = (y >> 255) & 1; y &= (1 << 255) - 1
    if y >= _P:
        return False
    y2 = (y * y) % _P; u = (y2 - 1) % _P; v = (_D * y2 + 1) % _P
    x = (u * pow(v, 3, _P) * pow((u * pow(v, 7, _P)) % _P, (_P - 5) // 8, _P)) % _P
    vx2 = (v * x * x) % _P
    if vx2 == (u % _P):
        pass
    elif vx2 == ((-u) % _P):
        x = (x * pow(2, (_P - 1) // 4, _P)) % _P
    else:
        return False
    return not (x == 0 and sign)
def _find_pda(seeds, program_id):
    for bump in range(255, -1, -1):
        h = hashlib.sha256()
        for s in seeds:
            h.update(s)
        h.update(bytes([bump])); h.update(program_id); h.update(b"ProgramDerivedAddress")
        d = h.digest()
        if not _on_curve(d):
            return d
    raise ValueError("no off-curve bump")
def _pda(client_ip, identity, program_id):
    return b58encode(_find_pda([b"doublezero", b"accesspass", socket.inet_aton(client_ip), identity], program_id))
def _exists(rpc, addr):
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "getAccountInfo", "params": [addr, {"encoding": "base64"}]}).encode()
    req = urllib.request.Request(rpc, data=body, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as r:
        resp = json.load(r)
    if "error" in resp:
        raise RuntimeError(resp["error"])
    val = resp.get("result", {}).get("value")
    if not val:
        return False
    data = val.get("data"); data = data[0] if isinstance(data, list) else data
    raw = base64.b64decode(data) if data else b""
    return len(raw) >= 1 and raw[0] == 11
def main():
    rpc = os.environ["DZ_RPC"]; program_id = b58decode(os.environ["DZ_PROG"]); pub_ip = os.environ.get("PUB_IP", "").strip()
    kp = json.loads(sys.stdin.read())
    if not isinstance(kp, list) or len(kp) != 64:
        print("keypair is not a 64-byte array", file=sys.stderr); return 4
    identity = bytes(kp[32:64]); print("IDENTITY " + b58encode(identity))
    try:
        for ip in ([pub_ip] if pub_ip else []) + ["0.0.0.0"]:
            if _exists(rpc, _pda(ip, identity, program_id)):
                print("found access pass bound to " + ip); return 0
    except Exception as e:
        print("RPC error: " + str(e), file=sys.stderr); return 4
    if pub_ip:
        print("no access pass for %s or 0.0.0.0" % pub_ip); return 2
    print("no 0.0.0.0 access pass and host public IP unknown"); return 3
sys.exit(main())
PY
))"
  AP_RC=$?
  set -e
  AP_ID="$(printf '%s\n' "$AP_OUT" | sed -n 's/^IDENTITY //p')"
  case "$AP_RC" in
    0) info "Access pass OK${AP_ID:+ (identity $AP_ID)} -- $(printf '%s\n' "$AP_OUT" | tail -1)";;
    2) die "Your keypair is not authorized to connect to DoubleZero ($DZ_ENV). Please contact DoubleZero to arrange access (a service contract) for this identity.
   Details for DoubleZero support: identity ${AP_ID:-unknown}, public IP ${AP_PUB_IP:-unknown}. (If that IP is not this host's real public IP, re-run with DZ_CLIENT_IP=<your public IP>.)";;
    3) warn "Could not determine this host's public IP and identity ${AP_ID:-?} has no 0.0.0.0 access pass; cannot confirm access. Set DZ_CLIENT_IP=<ip> to verify. Continuing.";;
    *) warn "Could not query the DoubleZero ledger ($DZ_RPC) to verify the access pass; continuing (\`doublezero connect\` will be the fallback).";;
  esac
fi

# ----------------------------------------------------------------------------
# 3. docker present? offer install
# ----------------------------------------------------------------------------
if ! command -v docker >/dev/null 2>&1; then
  warn "Docker is not installed."
  if confirm "Install Docker now via get.docker.com?"; then
    info "Installing Docker..."
    curl -fsSL https://get.docker.com | $SUDO sh
    $SUDO systemctl enable --now docker 2>/dev/null || true
  else
    die "Docker is required. Install it and re-run."
  fi
fi
$SUDO docker info >/dev/null 2>&1 || die "Docker is installed but the daemon isn't reachable. Start it (e.g. 'sudo systemctl start docker') and re-run."

# ----------------------------------------------------------------------------
# 4. host kernel / network prep (host-side; safe to attempt)
# ----------------------------------------------------------------------------
info "Preparing host for DoubleZero Edge Connect"
$SUDO modprobe tun 2>/dev/null    || warn "Could not load 'tun' module (may be built-in)."
$SUDO modprobe ip_gre 2>/dev/null || warn "Could not load 'ip_gre' module (will auto-load on tunnel create)."
[ -e /dev/net/tun ] || warn "/dev/net/tun is missing; tunnel creation may fail."

# The bridge wants a large SO_RCVBUF for bursty multicast; raise the host ceiling
# best-effort so the in-container setsockopt isn't silently clamped. Never fatal.
if [ "$($SUDO sysctl -n net.core.rmem_max 2>/dev/null || echo 0)" -lt "$RECV_BUF_MAX" ]; then
  $SUDO sysctl -w net.core.rmem_max="$RECV_BUF_MAX" >/dev/null 2>&1 \
    || warn "Could not raise net.core.rmem_max to $RECV_BUF_MAX (the bridge will use the host's current ceiling)."
fi

# best-effort firewall hints (don't auto-edit the user's firewall)
if command -v ufw >/dev/null 2>&1 && $SUDO ufw status 2>/dev/null | grep -qi "Status: active"; then
  warn "ufw is active: ensure IP protocol 47 (GRE) and UDP $LIVENESS_UDP_PORT are allowed."
fi
if command -v firewall-cmd >/dev/null 2>&1 && $SUDO firewall-cmd --state 2>/dev/null | grep -qi running; then
  warn "firewalld is running: ensure GRE (protocol 47) and UDP $LIVENESS_UDP_PORT are allowed."
fi

# ----------------------------------------------------------------------------
# 5. cloud detection -> warn about provider-level firewall (script can't fix)
# ----------------------------------------------------------------------------
detect_cloud() {
  local md="http://169.254.169.254"
  # AWS IMDSv2
  local tok
  tok=$(curl -fsS -m 1 -X PUT "$md/latest/api/token" -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' 2>/dev/null || true)
  if [ -n "$tok" ] && curl -fsS -m 1 -H "X-aws-ec2-metadata-token: $tok" "$md/latest/meta-data/instance-id" >/dev/null 2>&1; then echo aws; return; fi
  if curl -fsS -m 1 -H 'Metadata-Flavor: Google' "$md/computeMetadata/v1/instance/id" >/dev/null 2>&1; then echo gcp; return; fi
  if curl -fsS -m 1 -H 'Metadata: true' "$md/metadata/instance?api-version=2021-02-01" >/dev/null 2>&1; then echo azure; return; fi
  echo none
}
CLOUD="$(detect_cloud)"
case "$CLOUD" in
  aws)   warn "AWS detected. GRE will not work until you (in AWS, NOT on this host): 1) allow inbound IP protocol 47 in the Security Group; 2) DISABLE the ENI source/dest check.";;
  gcp)   warn "GCP detected. Add a firewall rule allowing IP protocol 47 (gre) to this instance.";;
  azure) warn "Azure detected. Add an NSG rule allowing IP protocol 47 to this VM.";;
esac

# ----------------------------------------------------------------------------
# 6. run the container (detached, long-lived: daemon + bridge)
# ----------------------------------------------------------------------------
info "Pulling $DZ_IMAGE ..."
$SUDO docker pull -q "$DZ_IMAGE" >/dev/null

info "Starting edge-connect bridge (env=$DZ_ENV)..."
$SUDO docker rm -f "$DZ_NAME" >/dev/null 2>&1 || true
# Bind-mount the keypair only when the secret was a file; a token-derived key is
# injected into the container after it starts (so it never touches the host disk).
mount_args=()
[ "$KEY_SRC" = file ] && mount_args=(-v "$KEYFILE":"$KEYPAIR_DEST":"$MNT_OPT")
# Relay bridge env vars to the container. The bridge reads every flag from an env var, so this is
# the only wiring needed to tune the WS sink, narrow feeds, or raise log level — no per-feature
# logic here. Only non-empty values are forwarded (so an empty override like WS_BIND="" to disable
# the WS sink can't be passed this way — use a hand-written `docker run` for that edge case).
PASSTHROUGH=(
  DZ_FEEDS DZ_IFACE DZ_RECV_BUF
  WS_BIND WS_HEARTBEAT_SECS WS_IDLE_TIMEOUT_SECS WS_MAX_CLIENTS
  WS_MAX_SUBS WS_MAX_INBOUND_PER_MIN WS_BROADCAST_CAPACITY
  DZ_SHRED_DEDUP_MODE DZ_SHRED_RPC_URL DZ_SHRED_FORWARD DZ_SHRED_SOURCES
  DZ_SHRED_CODE_PREFIX DZ_SHRED_PORT DZ_SHRED_DEDUP_WINDOW_SLOTS
  RUST_LOG
)
env_args=()
for v in "${PASSTHROUGH[@]}"; do
  [ -n "${!v:-}" ] && env_args+=(-e "$v=${!v}")
done
$SUDO docker run -d --name "$DZ_NAME" \
  --restart unless-stopped \
  --stop-timeout 60 \
  --network host \
  --cap-add NET_ADMIN --cap-add NET_RAW \
  --device /dev/net/tun \
  -e DZ_ENV="$DZ_ENV" \
  "${env_args[@]}" \
  "${mount_args[@]}" \
  "$DZ_IMAGE" >/dev/null

# wait for the daemon socket (the entrypoint logs "doublezerod ready" before the bridge starts)
info "Waiting for the daemon..."
for _ in $(seq 1 30); do
  $SUDO docker logs "$DZ_NAME" 2>&1 | grep -q "doublezerod ready" && break
  $SUDO docker ps -q --filter "name=^${DZ_NAME}$" | grep -q . || die "Container exited early. Logs: sudo docker logs $DZ_NAME"
  sleep 1
done

# Deliver a token-derived key by piping it straight into the container's
# filesystem, so the plaintext keypair lives only in the container (and is gone
# when the container is removed). A file secret is already bind-mounted above.
if [ "$KEY_SRC" = token ]; then
  printf '%s' "$KEY_JSON" | $SUDO docker exec -i "$DZ_NAME" \
    sh -c 'umask 077; mkdir -p /root/.config/doublezero && cat > /root/.config/doublezero/id.json'
  info "Installed keypair into the container (not stored on the host)."
fi

# ----------------------------------------------------------------------------
# 7. connect (always `doublezero connect multicast`)
# ----------------------------------------------------------------------------
# Give a cold daemon a head start on device probing before connect (see spin_sleep).
spin_sleep 15 "Letting the daemon finish bootstrapping"
info "Connecting: doublezero connect multicast"
# Allocate a pseudo-TTY when our stdout is a terminal so the CLI streams its
# normal output to the screen (without -t, docker exec gives it no TTY and the
# command's output is suppressed).
EXEC_TTY=""; [ -t 1 ] && EXEC_TTY="-t"
# NOTE: connect requires the host's public IP to have an access pass / allowlisted
# user onchain for $DZ_ENV. If this errors with an access-pass message, that
# provisioning step still needs to happen. Once the tunnel is up, the bridge
# self-heals onto the doublezero1 interface within ~30s and quotes begin flowing.
$SUDO docker exec $EXEC_TTY "$DZ_NAME" doublezero connect multicast || warn "connect failed (often: no access pass for this IP, or provider firewall/NAT). See notes above."

# ----------------------------------------------------------------------------
# 8. status + management hints
# ----------------------------------------------------------------------------
HOST_IP="$(hostname -I 2>/dev/null | awk '{print $1}')"; HOST_IP="${HOST_IP:-localhost}"
# Reflect any WS_BIND override in the printed URL (take the port after the last colon).
[ -n "${WS_BIND:-}" ]  && WS_PORT="${WS_BIND##*:}"
echo
# Brief pause so the freshly-connected tunnel settles before we read status.
spin_sleep 5 "Waiting for the tunnel to settle"
$SUDO docker exec "$DZ_NAME" doublezero status || true
echo
info "Done. The bridge is serving normalized quotes:"
echo "  WebSocket : ws://${HOST_IP}:${WS_PORT}            # normalized quotes (see PROTOCOL.md)"
echo
info "Manage with:"
echo "  sudo docker logs -f $DZ_NAME                            # bridge + daemon logs"
echo "  sudo docker exec -it $DZ_NAME doublezero status         # tunnel status"
echo "  sudo docker exec -it $DZ_NAME doublezero latency        # device latencies"
echo "  sudo docker stop $DZ_NAME && sudo docker rm $DZ_NAME    # disconnect, stop & remove"
