# Unit tests for the `connect*.sh` installers — design

**Date:** 2026-07-06
**Status:** approved (design), pending implementation plan
**Scope:** `scripts/connect.sh`, `scripts/connect-testnet.sh`, `scripts/connect-devnet.sh`

## Motivation

The three installers carry non-trivial logic (`DZ_` token decode, an embedded
Python access-pass/PDA check, WS-port preflight, env passthrough) and ship to users
verbatim over a CDN (`get.doublezero.xyz/connect*`). A silent regression recently
shipped undetected: `port_in_use "$p"; local rc=$?` exits the whole script under
`set -e` when the port is free (the common case), because the bare non-zero return
trips `errexit` before `rc` is captured. Nothing caught it — there are no shell
tests, and `shellcheck` does not reliably flag this specific `set -e` footgun.

Goal: behavioral unit tests around the pure logic and the exact failure mode we hit,
runnable in CI, without changing the runtime contract (each installer stays a single
self-contained file served over `curl | bash`).

## Design decisions (approved)

1. **Testability = source guard**, not a shared lib. Each script stays an independent
   file; a guard lets tests `source` it to get the functions without running `main`.
2. **Framework = bats-core.**
3. **Coverage buckets:** (a) `set -e` regression, (b) `DZ_` token parsing,
   (c) WS-port logic + env passthrough, (d) the embedded Python access-pass checker.
4. **The guard must not break `curl | bash`** — use behavioral source-detection, not
   `BASH_SOURCE == $0` (approved point 1).
5. **Extract `dz_token_to_json` and `build_env_args`** into functions (approved point 2).
6. **Mock RPC** for the Python network exit codes 0/2/3 (approved point 3).

## Script changes (all three, identical)

### The source guard — critical detail

The idiomatic `[ "${BASH_SOURCE[0]}" = "$0" ]` guard is **unsafe here**: under
`curl -fsSL … | bash` the script is read from stdin, `BASH_SOURCE[0]` is empty and
`$0` is `"bash"`, so the guard would misfire and abort the installer immediately —
worse than the bug we fixed. Use behavioral detection instead:

```bash
# after all function definitions, before section 1:
if (return 0 2>/dev/null); then return 0; fi   # sourced -> only funcs defined, stop
```

`return` succeeds only when sourced; executed (including via stdin from `curl|bash`)
it fails, so `main` runs. This is verified by a dedicated regression test
(`guard.bats`): executing via stdin proceeds past the guard; `source` stops before it.

### Reorganization (minimal)

- Move the `ws_disabled` / `ws_port` / `port_in_use` / `preflight_ws_port` block up
  next to `info`/`warn`/`ask`/… so all functions precede the guard.
- Add the two extractions below, also above the guard.
- Place the guard immediately before section 1 (the first executable statement).
- Sections 1–8 stay line-for-line the same except the two extracted blocks become
  function calls.

On `source`: `set` flags run, config defaults run (harmless var assignments), color
detection runs, all functions get defined, guard returns. **No `sudo`, `docker`, or
network side effects.**

### Two extractions (to make them testable)

- `dz_token_to_json <token>` — emits the 64-int JSON array, returns non-zero on invalid
  base64url length / wrong byte count. **Pure: no `die`/`info`** (the main flow keeps
  the messaging). Replaces the inline block (current `connect.sh:157–175`).
- `build_env_args` — prints the `-e VAR=val` arguments from the environment (the
  `PASSTHROUGH` loop plus the special "forward `WS_BIND` even when empty" rule).
  Replaces the inline block (current `connect.sh:487–502`).

## Test architecture

```
tests/scripts/
  _helpers.bash          # SCRIPTS list, stub-PATH builder, Python-heredoc extractor, mock-RPC starter
  guard.bats             # source stops at guard; stdin-exec runs main (blinds the curl|bash guard)
  set_e_regression.bats  # preflight_ws_port with a free port -> script survives (the bug)
  token_parse.bats       # dz_token_to_json: valid vectors + rejections
  ws_port.bats           # ws_disabled/ws_port/port_in_use + preflight decision table + build_env_args
  accesspass.bats        # embedded Python checker (extract heredoc + mock RPC)
```

`cargo` compiles only `tests/*.rs`; `tests/scripts/*.bats|*.bash` are ignored — they
coexist without collision.

### Isolation pattern

Each assertion runs in a fresh subshell with a stub-first `PATH` (fake `ss`, `docker`,
`sudo`, `netstat`, `curl`):

```bash
run bash -c 'source "$SCRIPT"; ws_port'
```

This keeps the script's `set -euo pipefail` from contaminating bats and exercises the
**real file** through its **real guard**.

### Drift protection across the three scripts

Because the files stay independent (no shared lib), the behavioral suite **iterates
over all three** (`SCRIPTS=(connect connect-testnet connect-devnet)`). A function that
diverges and breaks in one script is caught — this is the net against the drift we
already observed (the log-cap block present only in `connect.sh`).

## Coverage detail

### (a) `set -e` regression — `set_e_regression.bats`
Run `preflight_ws_port` with a stubbed `ss` reporting the port **free**; assert the
sourced script survives (status 0, execution continues). Fails against the old
`cmd; local rc=$?`, passes with `local rc=0; port_in_use || rc=$?`. Run for all three.

### (b) Token parse — `token_parse.bats`
`dz_token_to_json`: a known `DZ_` token → exact 64-int JSON; padding variants
(len %4 == 2 and == 3); reject len %4 == 1; reject a token that decodes to ≠ 64 bytes.

### (c) WS-port logic + env passthrough — `ws_port.bats`
- `ws_disabled`: unset / non-empty / set-empty (`WS_BIND=""`).
- `ws_port`: default 8081 / port from `WS_BIND`.
- `port_in_use`: anchoring (8081 must **not** match 18081), `ss` present, `netstat`
  fallback, neither → rc 2.
- `preflight_ws_port` decision table: free / in-use / no-tooling / disabled, plus the
  non-interactive continue and the interactive `p`/`d`/`c` branches (feed answers on a
  fake TTY; `p` re-runs the recursion on the new port).
- `build_env_args`: only non-empty vars forwarded; `WS_BIND` forwarded even when empty.

### (d) Embedded Python checker — `accesspass.bats`
The script stays single-file. The test helper extracts the `<<'PY' … PY` heredoc into a
temp `.py` **at test time** (real code, not a maintained copy) and runs it:
- **No network:** exit 5 (stdin not a 64-int array); exit 4 (RPC not http(s));
  "ignoring malformed public IP"; and an **identity vector** (known 64-byte keypair →
  expected `IDENTITY <base58>`, validating `b58encode` + the identity slice).
- **With network:** a local `http.server` mock returns a canned `getAccountInfo` —
  `data[0]==11` ⇒ exit 0; `value:null` ⇒ exit 2 (pub_ip set) / exit 3 (no pub_ip).

## CI

New `.github/workflows/shell-tests.yml` (sibling of `actionlint.yml`), triggered on PRs
touching `scripts/**` or `tests/scripts/**`: installs `bats` + `python3`, runs
`bats tests/scripts/`.

## Out of scope (YAGNI)

`detect_cloud` / `detect_public_ip` (network), the real `docker run`, sudo priming, and
`ask`/`confirm` TTY interaction beyond the non-interactive preflight path.
