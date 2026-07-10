#!/usr/bin/env bash
#
# DoubleZero Edge Connect installer (devnet)
# ------------------------------------------
# Served from https://get.doublezero.xyz/connect-devnet and run as:
#
#     curl -fsSL https://get.doublezero.xyz/connect-devnet | bash
#
# It checks for Docker (offering to install it), preps the host for GRE, loads the
# access secret, runs the edge-connect bridge container
# (ghcr.io/malbeclabs/doublezero-edge-connect-devnet:latest), runs
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
#   DZ_ENV=testnet|devnet|mainnet-beta   default: devnet
#   DZ_IMAGE=ghcr.io/malbeclabs/doublezero-edge-connect-devnet:latest
#   DZ_NAME=doublezero-edge-connect      container name
#   DZ_GHCR_TOKEN=<token>   ghcr token with read:packages (required: the devnet
#                           image is private). DZ_GHCR_USER defaults to malbeclabs.
#   DZ_GHCR_USER=<user>     optional; ghcr username for the login (default: malbeclabs)
#   DZ_FEEDS=<venue,venue>               optional: narrow ingested venues (default: all)
#   DZ_ASSUME_YES=1                      skip confirmation prompts (e.g. Docker install)
#   DZ_CLIENT_IP=<ipv4>                  override the public IP used by the access-pass pre-check
#   DZ_LEDGER_RPC_URL=<url>              override the DoubleZero ledger RPC used by the pre-check
#
# Any other bridge env var set in the environment is relayed straight to the container
# (WS_*, DZ_IFACE, DZ_SHRED_*, RUST_LOG, ...), so every binary feature can be tuned from the one-liner, e.g.:
#   WS_BIND=0.0.0.0:9000 curl -fsSL https://get.doublezero.xyz/connect-devnet | bash
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
DZ_IMAGE="${DZ_IMAGE:-ghcr.io/malbeclabs/doublezero-edge-connect-devnet:latest}"
DZ_NAME="${DZ_NAME:-doublezero-edge-connect}"
DZ_ENV="${DZ_ENV:-devnet}"
DZ_SECRET="${DZ_SECRET:-}"
DZ_FEEDS="${DZ_FEEDS:-}"
DZ_ASSUME_YES="${DZ_ASSUME_YES:-0}"
DZ_GHCR_TOKEN="${DZ_GHCR_TOKEN:-}"
DZ_GHCR_USER="${DZ_GHCR_USER:-malbeclabs}"
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
# Environment: default devnet, override via DZ_ENV; never prompted.
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
  # Whether the public IP was asserted by the operator (explicit) or merely guessed
  # (auto): a confirmed miss only hard-aborts when the operator asserted the IP.
  if [ -n "${DZ_CLIENT_IP:-}" ]; then AP_IP_SRC=explicit; else AP_IP_SRC=auto; fi
  # Feed the 64-byte keypair to python on stdin only (never argv/env), preserving the
  # "plaintext key is never printed or written to host disk" rule; only the public key is shown.
  # The python program is delivered via a /dev/fd process substitution so stdin stays free for the key.
  set +e
  # Read the keyfile under `set +e`: a root-owned 0600 key is readable by the docker
  # mount (root) but maybe not by this user -- degrade to a warning, never abort the install.
  if [ "$KEY_SRC" = token ]; then AP_FEED="$KEY_JSON"; else AP_FEED="$(cat "$KEYFILE" 2>/dev/null)"; fi
  AP_OUT="$(printf '%s' "$AP_FEED" | DZ_RPC="$DZ_RPC" DZ_PROG="$DZ_PROG" PUB_IP="$AP_PUB_IP" DZ_ENV="$DZ_ENV" python3 <(cat <<'PY'
# Host-side access-pass check. stdin: JSON array of the 64 keypair bytes (identity = last 32).
# env: DZ_RPC (ledger JSON-RPC), DZ_PROG (serviceability program id, base58), PUB_IP (this host's
# public IP, "" if unknown). Prints "IDENTITY <b58>" then a verdict line. Exit codes:
#   0 pass found   2 confirmed miss (PUB_IP known; neither it nor 0.0.0.0 has a pass)
#   3 inconclusive (PUB_IP unknown and no 0.0.0.0 pass)   4 inconclusive (RPC/decode error)
#   5 keypair could not be read/parsed (not a 64-int JSON array)
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
def _norm_ip(ip):
    # Strict dotted-quad check: inet_aton alone accepts 3-part ("1.2.3") and some
    # trailing-junk forms, so round-trip through inet_ntoa and require an exact match.
    try:
        return ip if socket.inet_ntoa(socket.inet_aton(ip)) == ip else None
    except OSError:
        return None
def main():
    rpc = os.environ["DZ_RPC"]; program_id = b58decode(os.environ["DZ_PROG"]); pub_ip = os.environ.get("PUB_IP", "").strip()
    if not rpc.startswith(("http://", "https://")):
        print("ledger RPC URL must be http(s)", file=sys.stderr); return 4
    try:
        kp = json.loads(sys.stdin.read())
        if not (isinstance(kp, list) and len(kp) == 64 and all(isinstance(b, int) and 0 <= b <= 255 for b in kp)):
            raise ValueError
    except Exception:
        print("keypair is not a 64-int JSON array", file=sys.stderr); return 5
    if pub_ip and _norm_ip(pub_ip) is None:
        print("ignoring malformed public IP " + repr(pub_ip), file=sys.stderr); pub_ip = ""
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
    2) if [ "$AP_IP_SRC" = explicit ]; then
         die "Your keypair is not authorized to connect to DoubleZero ($DZ_ENV) from $AP_PUB_IP. Please contact DoubleZero to arrange access (a service contract) for this identity.
   Details for DoubleZero support: identity ${AP_ID:-unknown}, public IP ${AP_PUB_IP:-unknown}."
       else
         warn "No access pass for this host's auto-detected public IP (${AP_PUB_IP:-unknown}) or 0.0.0.0; the pass may be bound to a different IP. If you know your public IP, re-run with DZ_CLIENT_IP=<your public IP>, or contact DoubleZero to arrange access (identity ${AP_ID:-unknown}). Continuing."
       fi;;
    3) warn "Could not determine this host's public IP and identity ${AP_ID:-?} has no 0.0.0.0 access pass; cannot confirm access. Set DZ_CLIENT_IP=<ip> to verify. Continuing.";;
    5) warn "Could not read or parse the keypair to verify the access pass (expected a 64-int JSON array); continuing (\`doublezero connect\` will be the fallback).";;
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
# 3b. existing instance? reinstall (graceful teardown) or cancel
# ----------------------------------------------------------------------------
# A prior run leaves a long-lived container (--restart unless-stopped) that holds
# the DoubleZero tunnel and the WS sink. Re-running the installer over a live
# instance would collide (same container name, same tunnel, same host ports), so
# detect one up front and tear it down cleanly first.
reinstall_existing_instance() {
  $SUDO docker ps -a -q --filter "name=^${DZ_NAME}$" 2>/dev/null | grep -q . || return 0

  local running="" existing_env="" existing_img=""
  $SUDO docker ps -q --filter "name=^${DZ_NAME}$" 2>/dev/null | grep -q . && running=1
  # Label the victim: all three installers share DZ_NAME, so e.g. the testnet
  # installer can find a live *mainnet* container -- name the network/image so the
  # operator isn't asked to destroy an unidentified instance.
  existing_env="$($SUDO docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "$DZ_NAME" 2>/dev/null | sed -n 's/^DZ_ENV=//p' | head -1 || true)"
  existing_img="$($SUDO docker inspect -f '{{.Config.Image}}' "$DZ_NAME" 2>/dev/null || true)"
  warn "An edge-connect instance ('$DZ_NAME'${existing_env:+, env=$existing_env}${existing_img:+, image=$existing_img}) already exists on this host${running:+ and is running}."

  # Decide whether to reinstall, keeping the three cases distinct so we neither
  # break headless automation nor mislabel a genuine decline:
  #   DZ_ASSUME_YES=1     -> reinstall (skip the prompt)
  #   interactive decline -> abort (the operator said no)
  #   no usable TTY, !yes -> reinstall, but say so (pre-3b behaviour was a silent
  #                          reinstall; keep automation working rather than abort)
  # A readable /dev/tty inode (-r) can still fail to OPEN with no controlling
  # terminal (cron/systemd/`curl|bash` without a tty), so probe an actual open
  # rather than trusting -r (and skip confirm()'s own tty read in that case).
  if [ "$DZ_ASSUME_YES" = 1 ]; then
    :
  elif { : <"$TTY"; } 2>/dev/null; then
    confirm "Reinstall? This disconnects and removes the existing instance" \
      || die "Cancelled: leaving the existing instance in place (manage it with 'sudo docker logs $DZ_NAME')."
  else
    warn "No terminal to prompt on and DZ_ASSUME_YES is unset; reinstalling to preserve non-interactive behaviour (set DZ_ASSUME_YES=1 to silence this, or run interactively to be asked first)."
  fi

  info "Uninstalling existing instance..."
  if [ -n "$running" ]; then
    # Graceful: `docker stop` sends SIGTERM, which the container entrypoint traps to
    # run a bounded `doublezero disconnect` (only if a tunnel is up) before tearing
    # doublezerod down -- releasing the GRE tunnel/routes/on-chain session cleanly.
    # The container's --stop-timeout (60s) bounds the stop; the outer `timeout`
    # guards a wedged docker CLI / restarting container so we can never hang forever.
    info "Stopping it gracefully (disconnecting the DoubleZero tunnel)..."
    local stop_ok=1
    if command -v timeout >/dev/null 2>&1; then
      $SUDO timeout 90 docker stop "$DZ_NAME" >/dev/null 2>&1 || stop_ok=0
    else
      $SUDO docker stop "$DZ_NAME" >/dev/null 2>&1 || stop_ok=0
    fi
    [ "$stop_ok" = 1 ] || warn "Could not stop the existing container cleanly (timed out or errored); forcing removal. Its GRE tunnel/routes may be orphaned in the host network namespace -- check 'doublezero status' / 'ip link' and disconnect manually if connectivity is off."
  fi
  $SUDO docker rm -f "$DZ_NAME" >/dev/null 2>&1 || true

  # Version-independent confirmation that the tunnel actually came down: the
  # container ran with --network host, so a failed disconnect leaves the doublezero1
  # interface (and its routes/on-chain session) orphaned in the host netns. Warn
  # loudly if it lingers -- the fresh connect below usually recreates it, but a
  # leftover old tunnel means the previous session wasn't released.
  if command -v ip >/dev/null 2>&1 && $SUDO ip link show doublezero1 >/dev/null 2>&1; then
    warn "The DoubleZero tunnel interface (doublezero1) is still present after teardown; the previous instance may not have disconnected cleanly. Its on-chain session/routes could be orphaned -- verify with 'doublezero status'."
  fi
}
reinstall_existing_instance

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

# best-effort firewall hints (don't auto-edit the user's firewall). Beyond GRE + the liveness
# port, a *default-deny-incoming* host also has to admit the decapsulated inner traffic: the GRE
# rule lets the outer encapsulated packets in, but once the kernel decapsulates them the inner
# multicast UDP re-traverses the INPUT chain on the tunnel interface (doublezero1) and is dropped
# unless that interface is allowed. The interface only exists once the tunnel is up, but the rule
# can be added ahead of time.
if command -v ufw >/dev/null 2>&1 && $SUDO ufw status 2>/dev/null | grep -qi "Status: active"; then
  warn "ufw is active: allow GRE (IP protocol 47) and UDP $LIVENESS_UDP_PORT, e.g.
       sudo ufw allow proto gre from any to any
       sudo ufw allow $LIVENESS_UDP_PORT/udp
     If your policy is default-deny-incoming, also admit the decapsulated inner multicast on the tunnel:
       sudo ufw allow in on doublezero1"
fi
if command -v firewall-cmd >/dev/null 2>&1 && $SUDO firewall-cmd --state 2>/dev/null | grep -qi running; then
  warn "firewalld is running: allow GRE (protocol 47) and UDP $LIVENESS_UDP_PORT. If your default zone denies incoming, also place the tunnel interface in a trusted zone once it exists:
       sudo firewall-cmd --zone=trusted --change-interface=doublezero1"
fi

# ----------------------------------------------------------------------------
# 4b. WebSocket sink port preflight (host-side)
# ----------------------------------------------------------------------------
# A taken WS port would make the bridge fail to bind that listener. The bridge now degrades
# gracefully (it logs a warning and runs without the sink — the DoubleZero tunnel is unaffected),
# but catch the conflict here too so the operator can pick another port or disable the sink up
# front instead of silently losing the WS output. Also defines ws_disabled/ws_port, reused by the
# env passthrough and the final status print.

# The WS sink is disabled when WS_BIND is set-but-empty (WS_BIND="").
ws_disabled() { [ "${WS_BIND+set}" = set ] && [ -z "${WS_BIND}" ]; }
# The WS port: WS_BIND's trailing :port when set non-empty, else the default WS_PORT.
ws_port() { if [ -n "${WS_BIND:-}" ]; then printf '%s' "${WS_BIND##*:}"; else printf '%s' "$WS_PORT"; fi; }

# Is a TCP port already bound on this host? Prefer ss, fall back to netstat; if neither exists we
# can't tell (rc 2 -> skip). A ':'/'.' immediately before the port anchors the match so e.g. 8081
# doesn't match 18081.
port_in_use() {
  local p="$1"
  if command -v ss >/dev/null 2>&1; then
    $SUDO ss -H -ltn 2>/dev/null | awk '{print $4}' | grep -qE "[:.]${p}\$"
  elif command -v netstat >/dev/null 2>&1; then
    $SUDO netstat -ltn 2>/dev/null | awk '{print $4}' | grep -qE "[:.]${p}\$"
  else
    return 2
  fi
}

preflight_ws_port() {
  ws_disabled && return 0                       # sink off -> nothing to bind
  local p; p="$(ws_port)"
  case "$p" in *[!0-9]*|'') return 0;; esac      # not a plain numeric port -> let the bridge validate WS_BIND
  local rc=0; port_in_use "$p" || rc=$?
  if [ "$rc" -eq 2 ]; then
    warn "Can't check whether TCP port $p is free (no ss/netstat installed); if it's in use the WS sink won't bind (the tunnel is unaffected)."
    return 0
  fi
  [ "$rc" -ne 0 ] && return 0                    # free
  # In use — name the holder when ss can show it.
  local who=""
  command -v ss >/dev/null 2>&1 && who="$($SUDO ss -Hltnp 2>/dev/null | awk -v p=":${p}\$" '$4 ~ p {print $NF; exit}')"
  warn "TCP port $p is already in use${who:+ ($who)}; the WS market-data sink can't bind there."
  info "The WS market-data sink is an OPTIONAL local WebSocket that re-serves ingested feed data to consumers on this host."
  info "A shred -> jito-shredstream setup does NOT use it; if nothing on this host consumes it, disabling is safe (it does not affect shred forwarding or the DoubleZero tunnel)."
  if [ -r "$TTY" ] && [ "$DZ_ASSUME_YES" != 1 ]; then
    local choice; choice="$(ask 'WS port in use — [p] pick another port, [d] disable the sink (clean: no sink, no bind error), [c] continue anyway (bridge starts but the sink fails to bind; fine only if you do not use it)' 'd')"
    case "$choice" in
      p|P) local np; np="$(ask 'New WS port' '8181')"; WS_BIND="0.0.0.0:${np}"; WS_PORT="$np"
           info "WS sink will use 0.0.0.0:${np}."; preflight_ws_port ;;   # re-check the new choice
      d|D) WS_BIND=""; info "WS sink disabled (WS_BIND=\"\")." ;;
      *)   warn "Continuing; the bridge starts without the WS sink (the tunnel is unaffected)." ;;
    esac
  else
    warn "Continuing non-interactively; the bridge starts without the WS sink (the tunnel is unaffected). Re-run with WS_BIND=<host>:<free-port> to serve it, or WS_BIND=\"\" to disable it explicitly."
  fi
}
preflight_ws_port

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
# The devnet image is private; authenticate to ghcr before pulling.
if [ -z "$DZ_GHCR_TOKEN" ]; then
  die "The devnet image is private. Set DZ_GHCR_TOKEN (a ghcr token with read:packages) and re-run."
fi
info "Logging in to ghcr.io as $DZ_GHCR_USER ..."
printf '%s' "$DZ_GHCR_TOKEN" | $SUDO docker login ghcr.io -u "$DZ_GHCR_USER" --password-stdin >/dev/null \
  || die "ghcr login failed. Check DZ_GHCR_TOKEN/DZ_GHCR_USER."

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
# logic here. Only non-empty values are forwarded, with one exception (WS_BIND, below).
PASSTHROUGH=(
  DZ_FEEDS DZ_IFACE DZ_RECV_BUF
  WS_HEARTBEAT_SECS WS_IDLE_TIMEOUT_SECS WS_MAX_CLIENTS
  WS_MAX_SUBS WS_MAX_INBOUND_PER_MIN WS_BROADCAST_CAPACITY
  DZ_SHRED_DEDUP_MODE DZ_SHRED_RPC_URL DZ_SHRED_FORWARD DZ_SHRED_SOURCES
  DZ_SHRED_CODE_PREFIX DZ_SHRED_PORT DZ_SHRED_DEDUP_WINDOW_SLOTS
  RUST_LOG
)
env_args=()
for v in "${PASSTHROUGH[@]}"; do
  [ -n "${!v:-}" ] && env_args+=(-e "$v=${!v}")
done
# WS_BIND is the one var forwarded whenever it is *set* — including set-but-empty (WS_BIND="") —
# so the WS sink can be disabled straight from the one-liner (an empty --ws-bind turns the sink
# off in the bridge). The preflight above may also have set/cleared it in response to a conflict.
[ "${WS_BIND+set}" = set ] && env_args+=(-e "WS_BIND=${WS_BIND}")
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
spin_sleep 30 "Letting the daemon finish bootstrapping"
info "Connecting: doublezero connect multicast"
# Allocate a pseudo-TTY when our stdout is a terminal so the CLI streams its
# normal output to the screen (without -t, docker exec gives it no TTY and the
# command's output is suppressed).
EXEC_TTY=""; [ -t 1 ] && EXEC_TTY="-t"
# NOTE: connect requires the host's public IP to have an access pass / allowlisted
# user onchain for $DZ_ENV. If this errors with an access-pass message, that
# provisioning step still needs to happen. Once the tunnel is up, the bridge
# self-heals onto the doublezero1 interface within ~30s and quotes begin flowing.
$SUDO docker exec $EXEC_TTY "$DZ_NAME" doublezero connect multicast || warn "connect failed (often: no access pass for this IP; provider firewall/NAT; or a default-deny host firewall dropping the decapsulated inner multicast on doublezero1). See the firewall notes above."

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
if ws_disabled; then
  info "Done. The WebSocket sink is disabled (WS_BIND=\"\"); the bridge ingests DZ Edge and (if configured) forwards shreds, but serves no WebSocket."
else
  # The WS sink and the shred forwarder are activated independently by the subscription reconciler
  # (docs/output-sinks.md), each only once this host is actually subscribed -- so a shreds-only host
  # serves no WebSocket, and a just-connected host may have neither yet. Rather than re-derive that
  # dynamic, per-feature state (it also depends on DZ_SHRED_SOURCES / DZ_SHRED_DISABLE), observe the
  # bridge's own decisions from its log (default warn,doublezero_edge_connect=info level):
  #   "activating WebSocket sink"                      -> quotes are being served
  #   "activating shred forwarder (subscribed groups)" -> shreds are being forwarded
  # A direct WS-port probe backstops the log line if RUST_LOG was lowered. The reconciler runs its
  # first post-connect pass within one refresh interval (DZ_SUBSCRIPTION_REFRESH_SECS, default 30s),
  # so wait up to that (capped) and stop as soon as either activates. The matching "inactive" log
  # lines aren't emitted for a feature that was never up, so inactivity is inferred from a full wait.
  refresh="${DZ_SUBSCRIPTION_REFRESH_SECS:-30}"; case "$refresh" in *[!0-9]*|'') refresh=30;; esac
  max_wait=$((refresh + 5)); if [ "$max_wait" -gt 35 ]; then max_wait=35; fi
  ws_up=""; shreds_up=""
  for _ in $(seq 1 "$max_wait"); do
    logs="$($SUDO docker logs "$DZ_NAME" 2>&1 || true)"
    if printf '%s' "$logs" | grep -q "activating WebSocket sink" || port_in_use "$WS_PORT"; then ws_up=1; fi
    if printf '%s' "$logs" | grep -q "activating shred forwarder"; then shreds_up=1; fi
    if [ -n "$ws_up" ] || [ -n "$shreds_up" ]; then break; fi
    sleep 1
  done
  if [ -n "$ws_up" ]; then
    info "Done. The bridge is serving normalized quotes:"
    echo "  WebSocket : ws://${HOST_IP}:${WS_PORT}            # normalized quotes (see PROTOCOL.md)"
  elif [ -n "$shreds_up" ]; then
    info "Done. Forwarding shreds. The WebSocket quote sink is idle -- it activates once this host is subscribed to a market-data feed."
    echo "  WebSocket : ws://${HOST_IP}:${WS_PORT}            # activates once >=1 market-data feed is subscribed"
  else
    info "Done. Connected. The WebSocket sink and shred forwarder each activate automatically once this host is subscribed to a group (a market-data feed for WS; an edge-solana-* group for shreds) -- allow up to one refresh interval (DZ_SUBSCRIPTION_REFRESH_SECS, default 30s). Check: sudo docker logs $DZ_NAME"
    echo "  WebSocket : ws://${HOST_IP}:${WS_PORT}            # once >=1 market-data feed is subscribed"
  fi
fi
echo
info "Manage with:"
echo "  sudo docker logs -f $DZ_NAME                            # bridge + daemon logs"
echo "  sudo docker exec -it $DZ_NAME doublezero status         # tunnel status"
echo "  sudo docker exec -it $DZ_NAME doublezero latency        # device latencies"
echo "  sudo docker stop $DZ_NAME && sudo docker rm $DZ_NAME    # disconnect, stop & remove"
