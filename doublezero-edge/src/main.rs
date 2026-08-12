//! `doublezero-edge`: an agent-facing CLI over a running `doublezero-edge-connect` container's
//! `/v1` HTTP market-data API. Command surface and JSON envelopes are modelled on the Coinbase
//! Advanced Trade CLI (`key==value` query parameters, `--jq`, `--template`) so an agent already
//! fluent in that tool needs no new vocabulary here.
//!
//! Every `products`/`status` command is READ-ONLY (a `GET` against `/v1`, which has no
//! order-placement or mutation path anywhere in `edge-connect` for it to reach) and never needs a
//! confirmation prompt. The commands over the **admin** surface (`--admin-url`) are where that
//! stops holding: `diagnose` is read-only too, but `channels set`, `connect` and `disconnect` all
//! mutate — what this process ingests, or the host's tunnel — so each states exactly what it will
//! do and requires confirmation unless `--force`.

use std::time::{Duration, Instant};

use clap::{CommandFactory, Parser, Subcommand};
use clap_complete::{generate, Shell};
use doublezero_edge::{channels, client, diagnose, endpoint::Endpoint, jq, params, render};
use serde_json::{json, Value};

/// doublezero-edge: an agent-facing CLI over a running edge-connect container's /v1 market-data
/// API (plus its admin surface, for `diagnose`, `channels`, `connect` and `disconnect`).
#[derive(Parser)]
#[command(
    name = "doublezero-edge",
    version,
    about = "Market-data client for a running doublezero-edge-connect container.",
    long_about = "doublezero-edge queries a running doublezero-edge-connect container's /v1 HTTP \
API for market data: products, tickers, candles, order books, best bid/ask, and feed health.\n\n\
`products` and `status` are READ-ONLY: every one of those is a GET against /v1, which has no \
order-placement or mutation path anywhere in edge-connect for it to reach, so neither ever needs a \
confirmation prompt — a real difference from the Coinbase Advanced Trade CLI this tool's surface \
is modelled on, where blast-radius containment (accidentally placing a real order) drives much of \
the design.\n\n\
`diagnose`, `channels`, `connect` and `disconnect` are separate: they talk to edge-connect's admin \
surface (--admin-url, on by default at 127.0.0.1:9098) rather than /v1, which matters because \
/v1 activates only once a market-data feed is subscribed — on a host whose tunnel never came up it \
is not listening at all, and `diagnose` is then the command that answers why. The three mutating \
ones (`channels set` replaces which channels this process ingests; `connect`/`disconnect` re-run \
the DoubleZero client verb inside the container) each state what they will do and require \
confirmation unless --force.\n\n\
Trailing arguments to a products subcommand are either the product id (a bare positional, e.g. \
HYPERLIQUID:BTC) or a key==value query parameter (e.g. granularity==ONE_MINUTE); the two are told \
apart by the presence of '==', matched before anything is treated as positional, so a query value \
that itself contains '=' still parses whole."
)]
struct Cli {
    /// Base URL of the edge-connect /v1 API.
    #[arg(
        long,
        global = true,
        env = "DOUBLEZERO_EDGE_URL",
        default_value = "http://127.0.0.1:9099"
    )]
    url: String,

    /// Base URL of the edge-connect admin surface (`--admin-bind` / `DZ_ADMIN_BIND` in the
    /// container). Unlike `/v1` it is not subscription-gated, so it answers on a host whose tunnel
    /// never came up — which is what `diagnose` is for.
    #[arg(long, global = true, env = "DOUBLEZERO_EDGE_ADMIN_URL", default_value = DEFAULT_ADMIN_URL)]
    admin_url: String,

    /// jq-subset filter applied to the JSON response, e.g. '.trades[0].price' or
    /// '.products[].product_id'. Applies to a successful response only; ignores --output.
    #[arg(long, global = true)]
    jq: Option<String>,

    /// Print the query parameters this command accepts instead of making a request.
    #[arg(long, global = true)]
    template: bool,

    /// Output format for a successful response. Errors are always the JSON envelope on stderr,
    /// regardless of this flag.
    #[arg(long, global = true, value_enum, default_value_t = OutputFormat::Json)]
    output: OutputFormat,

    #[command(subcommand)]
    command: Command,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
enum OutputFormat {
    Json,
    Table,
}

#[derive(Subcommand)]
enum Command {
    /// Query the product catalog and its market data.
    Products {
        #[command(subcommand)]
        action: ProductsCommand,
    },
    /// Feed health and history-buffer retention state.
    Status {
        #[arg(num_args = 0.., value_name = "ARGS")]
        args: Vec<String>,
    },
    /// Inspect or change which channels of an enabled feed this process ingests. Talks to
    /// edge-connect's admin surface (`--admin-url` / `DZ_ADMIN_BIND`), not `/v1` — see the
    /// top-level `--help` for why this command group is separate.
    Channels {
        #[command(subcommand)]
        action: ChannelsCommand,
    },
    /// Why this container is (or is not) serving data: the tunnel, what this host is subscribed
    /// to, and which feeds are activated, ending in one verdict. Reads the admin surface, which
    /// answers even when `/v1` is not activated — so this is the command to run when everything
    /// else reports `api_unreachable`. Exits 0 whatever the verdict; 3 only if the admin surface
    /// itself does not answer.
    Diagnose,
    /// Ask the container to run `doublezero connect multicast` — the retry path for a `tunnel_down`
    /// verdict, without a `docker exec`. Mutating: it provisions this host's onchain DoubleZero
    /// user, so it states what it will run and confirms unless `--force`.
    Connect(AttemptArgs),
    /// Ask the container to run `doublezero disconnect multicast`, tearing this host's tunnel
    /// down. Mutating and disruptive — see the prompt it prints.
    Disconnect(AttemptArgs),
    /// Print a shell-completion script for `<shell>` to stdout. Local-only — no config file, no
    /// server, no network — so packaging can run it at build time.
    Completion {
        #[arg(value_enum)]
        shell: Shell,
    },
}

/// The flags `connect` and `disconnect` share. They differ only in the verb they run and the
/// warning they print, so the argument surface is one type.
#[derive(clap::Args)]
struct AttemptArgs {
    /// Skip the confirmation prompt. For a non-interactive caller — interactively, answer the
    /// prompt.
    #[arg(long)]
    force: bool,
    /// Return as soon as the container accepts the request, without waiting for the client verb to
    /// finish. Its outcome then shows up under `last_attempt` in `doublezero-edge diagnose`.
    #[arg(long)]
    no_wait: bool,
    /// Seconds to wait for the client verb to finish before giving up on watching it (it keeps
    /// running in the container regardless).
    #[arg(long, default_value_t = 180)]
    timeout: u64,
}

/// The admin surface's base URL — `--admin-bind`'s own default, which is loopback-only by design.
const DEFAULT_ADMIN_URL: &str = "http://127.0.0.1:9098";

/// How often `connect`/`disconnect` re-read `/admin/diagnostics` while waiting. The client verb
/// takes minutes; this is only how promptly its end is noticed.
const ATTEMPT_POLL_INTERVAL: Duration = Duration::from_secs(2);

/// Page size `products list --paginate` supplies on the caller's behalf when no `limit==N` was
/// given — pagination proves nothing in one unbounded request, so `--paginate` must set a page
/// size itself to actually walk more than one page. A caller's own `limit==N` always wins.
const DEFAULT_PAGINATE_LIMIT: u32 = 500;

#[derive(Subcommand)]
enum ChannelsCommand {
    /// List each enabled row's channels: channel-filter admission, real bound state, product count.
    List,
    /// Replace the channel filter (same spec syntax as `--channels`/`DZ_CHANNELS`:
    /// `<code>=<id>[,<id>...][;<code>=...]`). Prints what would be dropped and requires
    /// confirmation unless `--force` — the drop is irreversible within the history window.
    Set {
        /// The new channel filter spec. An empty string clears every restriction.
        spec: String,
        /// Skip the drop preview's confirmation prompt.
        #[arg(long)]
        force: bool,
    },
}

#[derive(Subcommand)]
enum ProductsCommand {
    /// List every known product. `--paginate` follows the server's `cursor` until the catalog is
    /// exhausted, accumulating every page into one response (`limit==N` sets the page size; the
    /// default is unlimited — a single response with no cursor at all).
    List {
        #[arg(num_args = 0.., value_name = "ARGS")]
        args: Vec<String>,
        /// Follow `cursor`s until the catalog is exhausted, accumulating every page into one
        /// response — matches the Coinbase Advanced Trade CLI's `--paginate`. Without it, a
        /// `limit==N` with more remaining returns just the first page and its `cursor`.
        #[arg(long)]
        paginate: bool,
    },
    /// One product's identity and registry-derived fields.
    Get {
        #[arg(num_args = 0.., value_name = "PRODUCT_ID_AND_ARGS")]
        args: Vec<String>,
    },
    /// Recent trades plus best bid/ask for one product.
    Ticker {
        #[arg(num_args = 0.., value_name = "PRODUCT_ID_AND_ARGS")]
        args: Vec<String>,
    },
    /// OHLCV candles for one product.
    Candles {
        #[arg(num_args = 0.., value_name = "PRODUCT_ID_AND_ARGS")]
        args: Vec<String>,
    },
    /// Order book (pricebook) for one product.
    Book {
        #[arg(num_args = 0.., value_name = "PRODUCT_ID_AND_ARGS")]
        args: Vec<String>,
    },
    /// Best bid/ask across products. Narrow it to one or more products with a bare positional id
    /// (e.g. `HYPERLIQUID:BTC`) or `product_ids==A,B`; omit both for every product.
    #[command(name = "best_bid_ask")]
    BestBidAsk {
        #[arg(num_args = 0.., value_name = "ARGS")]
        args: Vec<String>,
    },
}

/// Which endpoint a parsed command targets, plus its raw trailing arguments (still a mix of
/// `key==value` params and positionals — `params::split` hasn't run yet).
fn resolve(command: &Command) -> (Endpoint, Vec<String>) {
    match command {
        Command::Status { args } => (Endpoint::Status, args.clone()),
        Command::Products { action } => match action {
            ProductsCommand::List { args, .. } => (Endpoint::ProductsList, args.clone()),
            ProductsCommand::Get { args } => (Endpoint::ProductGet, args.clone()),
            ProductsCommand::Ticker { args } => (Endpoint::Ticker, args.clone()),
            ProductsCommand::Candles { args } => (Endpoint::Candles, args.clone()),
            ProductsCommand::Book { args } => (Endpoint::Book, args.clone()),
            ProductsCommand::BestBidAsk { args } => (Endpoint::BestBidAsk, args.clone()),
        },
        Command::Channels { .. }
        | Command::Diagnose
        | Command::Connect(_)
        | Command::Disconnect(_)
        | Command::Completion { .. } => {
            unreachable!("this command is dispatched directly by run(), never through resolve()")
        }
    }
}

/// Build the request path for `endpoint`, pulling the product id (percent-encoded) from
/// `positionals` where one is required. A missing id is a usage error (exit 1), not a network
/// call that would only fail later.
fn build_path(endpoint: Endpoint, positionals: &[String]) -> Result<String, String> {
    let require_id = || -> Result<String, String> {
        match positionals.first() {
            Some(id) if !id.is_empty() => Ok(client::encode_path_segment(id)),
            _ => Err("missing required <product_id> argument".to_string()),
        }
    };
    Ok(match endpoint {
        Endpoint::ProductsList => "/v1/products".to_string(),
        Endpoint::BestBidAsk => "/v1/best_bid_ask".to_string(),
        Endpoint::Status => "/v1/status".to_string(),
        Endpoint::ProductGet => format!("/v1/products/{}", require_id()?),
        Endpoint::Ticker => format!("/v1/products/{}/ticker", require_id()?),
        Endpoint::Candles => format!("/v1/products/{}/candles", require_id()?),
        Endpoint::Book => format!("/v1/products/{}/book", require_id()?),
        Endpoint::ChannelsList
        | Endpoint::ChannelsSet
        | Endpoint::Diagnose
        | Endpoint::Connect
        | Endpoint::Disconnect => {
            unreachable!(
                "admin-surface commands are dispatched directly by run(), never through build_path"
            )
        }
    })
}

/// `best_bid_ask` accepts a product id two ways — every sibling subcommand (`ticker`, `candles`,
/// `book`, `get`) takes it as a bare positional, so this one does too, alongside the faithful
/// `product_ids==A,B` form. A bare positional is folded into `product_ids` (appended to whatever
/// the caller already gave via `product_ids==...`, so the two forms compose rather than one
/// silently winning); no positional and no `product_ids` param leaves `params` untouched, keeping
/// today's "every product" behaviour.
fn merge_best_bid_ask_params(
    params: Vec<(String, String)>,
    positionals: &[String],
) -> Vec<(String, String)> {
    let mut ids: Vec<String> = Vec::new();
    let mut out = Vec::new();
    for (k, v) in params {
        if k == "product_ids" {
            ids.extend(v.split(',').map(|s| s.to_string()));
        } else {
            out.push((k, v));
        }
    }
    if let Some(id) = positionals.first() {
        if !id.is_empty() {
            ids.push(id.clone());
        }
    }
    if !ids.is_empty() {
        out.push(("product_ids".to_string(), ids.join(",")));
    }
    out
}

fn main() {
    std::process::exit(run());
}

/// Every stdout write in this binary routes through this handle instead of bare `println!`/
/// `print!`. Rust ignores `SIGPIPE` at startup, so once a downstream reader goes away early
/// (`| head`, `| less -q`, a `grep -q` that stops after its first match — completely ordinary
/// for a JSON/table-printing CLI meant to be composed), the next write returns a `BrokenPipe`
/// error instead of killing the process via the signal, and `println!`'s own writer treats that
/// error as an `expect()`-worthy bug and panics. This crate is deliberately dependency-light (see
/// `Cargo.toml`), so rather than add `libc` just to restore the default `SIGPIPE` disposition,
/// [`StdoutOrExit`] catches `BrokenPipe` itself and exits at code 0 — the far end closing early is
/// normal operation here, not a failure.
struct StdoutOrExit(std::io::Stdout);

fn stdout_or_exit() -> StdoutOrExit {
    StdoutOrExit(std::io::stdout())
}

impl std::io::Write for StdoutOrExit {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self.0.write(buf) {
            Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => std::process::exit(0),
            result => result,
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self.0.flush() {
            Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => std::process::exit(0),
            result => result,
        }
    }
}

/// Write `line` + `\n` to stdout, exiting cleanly (see [`StdoutOrExit`]) rather than panicking if
/// the reader on the other end of a pipe has already gone away.
fn println_or_exit(line: &str) {
    use std::io::Write;
    let _ = writeln!(stdout_or_exit(), "{line}");
}

/// As [`println_or_exit`], without the trailing newline — for the one place (the `channels set`
/// confirmation prompt) that needs the cursor to stay on the same line; flushes so the prompt is
/// visible before the subsequent `stdin` read.
fn print_or_exit(text: &str) {
    use std::io::Write;
    let mut out = stdout_or_exit();
    let _ = write!(out, "{text}");
    let _ = out.flush();
}

fn run() -> i32 {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(e) => return handle_clap_error(&e),
    };

    if let Command::Completion { shell } = &cli.command {
        // Local-only: no config file, no server, no network — must return before any HTTP client
        // is built so packaging can run this at build time.
        return run_completion(*shell);
    }

    // The admin-surface commands: a distinct surface (`--admin-url`) with their own confirmation
    // flows, handled separately from the /v1 GET pipeline below.
    match &cli.command {
        Command::Channels { action } => return run_channels(&cli, action),
        Command::Diagnose => return run_diagnose(&cli),
        Command::Connect(args) => return run_attempt(&cli, args, Attempt::Connect),
        Command::Disconnect(args) => return run_attempt(&cli, args, Attempt::Disconnect),
        _ => {}
    }

    if let Command::Products {
        action: ProductsCommand::List {
            args,
            paginate: true,
        },
    } = &cli.command
    {
        // Drives more than one request itself (following `cursor`), so it can't go through the
        // generic single-request pipeline below.
        return run_products_list_paginated(&cli, args);
    }

    let (endpoint, raw_args) = resolve(&cli.command);
    let (params, positionals) = params::split(&raw_args);

    if cli.template {
        return emit_template(&cli, &endpoint.template());
    }

    let path = match build_path(endpoint, &positionals) {
        Ok(path) => path,
        Err(msg) => {
            eprintln!("error: {msg}");
            return 1;
        }
    };

    let params = if endpoint == Endpoint::BestBidAsk {
        merge_best_bid_ask_params(params, &positionals)
    } else {
        params
    };

    let client = match build_http_client() {
        Ok(client) => client,
        Err(msg) => {
            eprintln!("error: {msg}");
            return 1;
        }
    };

    match client::get(&client, &cli.url, &path, &params) {
        client::Outcome::Ok { body } => emit(&cli, endpoint, &body),
        client::Outcome::Failed { status, body } => {
            print_error(&body);
            exit_code_for_status(status)
        }
        client::Outcome::Unreachable { body } => print_v1_unreachable(&cli, &client, &body),
        client::Outcome::Invalid { body, .. } => {
            print_error(&body);
            3
        }
    }
}

/// Report a `/v1` transport failure, telling the two causes apart: a container that is not running,
/// and one that is running with `/v1` not activated (it is subscription-gated, so on a host whose
/// tunnel never came up it is not listening at all — the failure that sends an operator to
/// `docker ps`, which shows a healthy container and no further clue).
///
/// The [`client::same_host`] guard is load-bearing: against a remote bridge with the default
/// loopback `--admin-url`, an unguarded probe would answer for the *local* container and report its
/// state as the remote one's — confidently wrong, which is worse than the vague message it replaces.
fn print_v1_unreachable(cli: &Cli, client: &reqwest::blocking::Client, body: &Value) -> i32 {
    let probed = client::same_host(&cli.url, &cli.admin_url)
        .then(|| client::probe_diagnostics(client, &cli.admin_url))
        .flatten();
    match probed {
        Some(diag) => {
            let summary = diag["diagnosis"]["summary"].as_str().unwrap_or_default();
            print_error(&client::api_inactive_envelope(&cli.url, summary));
        }
        None => print_error(body),
    }
    3
}

/// Build the one HTTP client every command (`/v1` and admin alike) shares.
fn build_http_client() -> Result<reqwest::blocking::Client, String> {
    reqwest::blocking::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|e| format!("failed to build HTTP client: {e}"))
}

/// `completion <shell>`: write a shell-completion script for `shell` to stdout. Local-only (no
/// config file, no server, no network), matching the sibling `doublezero` CLI's own `completion`
/// command so the two tools feel identical.
fn run_completion(shell: Shell) -> i32 {
    let mut cmd = Cli::command();
    let name = cmd.get_name().to_string();
    generate(shell, &mut cmd, name, &mut stdout_or_exit());
    0
}

/// `products list --paginate`: follow the server's `cursor` until the catalog is exhausted,
/// accumulating every page's `products` into one response — same envelope shape as an unpaginated
/// call (no `cursor` key), so `--jq`/`--output`/`--template` behave identically either way.
fn run_products_list_paginated(cli: &Cli, args: &[String]) -> i32 {
    if cli.template {
        return emit_template(cli, &Endpoint::ProductsList.template());
    }

    let (mut params, _positionals) = params::split(args);
    // A caller's own `limit==N` wins; otherwise supply this CLI's own page size, since one
    // unbounded request would return everything in a single page and `--paginate` would issue
    // exactly one request — proving nothing about the flag. We drive the cursor ourselves, so any
    // `cursor==...` the caller passed is dropped rather than silently starting mid-catalog.
    if !params.iter().any(|(k, _)| k == "limit") {
        params.push(("limit".to_string(), DEFAULT_PAGINATE_LIMIT.to_string()));
    }
    params.retain(|(k, _)| k != "cursor");

    let client = match build_http_client() {
        Ok(client) => client,
        Err(msg) => {
            eprintln!("error: {msg}");
            return 1;
        }
    };

    let mut accumulated: Vec<Value> = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let mut req_params = params.clone();
        if let Some(c) = &cursor {
            req_params.push(("cursor".to_string(), c.clone()));
        }
        match client::get(&client, &cli.url, "/v1/products", &req_params) {
            client::Outcome::Ok { body } => {
                if let Some(products) = body.get("products").and_then(Value::as_array) {
                    accumulated.extend(products.iter().cloned());
                }
                cursor = body
                    .get("cursor")
                    .and_then(Value::as_str)
                    .filter(|c| !c.is_empty())
                    .map(str::to_string);
                if cursor.is_none() {
                    break;
                }
            }
            client::Outcome::Failed { status, body } => {
                print_error(&body);
                return exit_code_for_status(status);
            }
            client::Outcome::Unreachable { body } => {
                return print_v1_unreachable(cli, &client, &body)
            }
            client::Outcome::Invalid { body, .. } => {
                print_error(&body);
                return 3;
            }
        }
    }

    emit(
        cli,
        Endpoint::ProductsList,
        &json!({ "products": accumulated }),
    )
}

/// `channels list` / `channels set`. Both talk to the admin surface (`--admin-url`), never
/// `cli.url` alone — though `list` additionally reads `cli.url`'s `/v1/status` for the real
/// per-channel bound state and product counts the admin surface itself does not report (see
/// `channels::render_channels_list`'s docs), and `set` reads it for the drop preview.
fn run_channels(cli: &Cli, action: &ChannelsCommand) -> i32 {
    let client = match build_http_client() {
        Ok(client) => client,
        Err(msg) => {
            eprintln!("error: {msg}");
            return 1;
        }
    };

    match action {
        ChannelsCommand::List => run_channels_list(cli, &client),
        ChannelsCommand::Set { spec, force } => run_channels_set(cli, &client, spec, *force),
    }
}

/// `channels list`: confirm the admin surface answers (naming `DZ_ADMIN_BIND` if it doesn't), then
/// merge its channel-filter summary with `/v1/status`'s real per-channel liveness into one JSON value so
/// `--jq`/`--output`/`--template` behave exactly as they do for every other command.
fn run_channels_list(cli: &Cli, client: &reqwest::blocking::Client) -> i32 {
    if cli.template {
        return emit_template(cli, &Endpoint::ChannelsList.template());
    }

    let admin_body = match client::admin_get(client, &cli.admin_url, "/admin/channels") {
        client::Outcome::Ok { body } => body,
        client::Outcome::Failed { status, body } => {
            print_error(&body);
            return exit_code_for_status(status);
        }
        client::Outcome::Invalid { body, .. } | client::Outcome::Unreachable { body } => {
            print_error(&body);
            return 3;
        }
    };

    let status_body = match client::get(client, &cli.url, "/v1/status", &[]) {
        client::Outcome::Ok { body } => body,
        client::Outcome::Failed { status, body } => {
            print_error(&body);
            return exit_code_for_status(status);
        }
        client::Outcome::Unreachable { body } => return print_v1_unreachable(cli, client, &body),
        client::Outcome::Invalid { body, .. } => {
            print_error(&body);
            return 3;
        }
    };

    let combined = serde_json::json!({ "admin": admin_body, "status": status_body });
    emit(cli, Endpoint::ChannelsList, &combined)
}

/// `channels set`: preview what the new channel filter would drop (best-effort, from `/v1/status`), require
/// confirmation unless `--force`, then `POST` the spec to the admin surface.
fn run_channels_set(cli: &Cli, client: &reqwest::blocking::Client, spec: &str, force: bool) -> i32 {
    if cli.template {
        return emit_template(cli, &Endpoint::ChannelsSet.template());
    }

    let preview = match client::get(client, &cli.url, "/v1/status", &[]) {
        client::Outcome::Ok { body } => match channels::compute_drops(&body, spec) {
            Ok(drops) => Some(drops),
            Err(msg) => {
                eprintln!("warning: could not compute the drop preview: {msg}");
                None
            }
        },
        client::Outcome::Failed { body, .. }
        | client::Outcome::Invalid { body, .. }
        | client::Outcome::Unreachable { body } => {
            eprintln!(
                "warning: could not fetch current status for the drop preview: {}",
                serde_json::to_string(&body).unwrap_or_default()
            );
            None
        }
    };

    match &preview {
        Some(drops) => println_or_exit(&channels::render_drop_preview(drops)),
        None => println_or_exit("(drop preview unavailable; proceeding on the spec alone)"),
    }

    if !force {
        if preview.is_none() {
            eprintln!(
                "error: cannot confirm this change without a drop preview. Pass --force to apply \
                 anyway, or fix --url so the preview can be computed."
            );
            return 1;
        }
        print_or_exit(
            "Apply this channel filter? Dropped channels' books/history/catalog entries are not \
                recoverable within the window. Type 'yes' to continue: ",
        );
        let mut input = String::new();
        let confirmed = std::io::stdin().read_line(&mut input).is_ok()
            && input.trim().eq_ignore_ascii_case("yes");
        if !confirmed {
            eprintln!("aborted; the channel filter was not changed.");
            return 1;
        }
    }

    match client::admin_post_channels(client, &cli.admin_url, spec) {
        client::Outcome::Ok { body } => emit(cli, Endpoint::ChannelsSet, &body),
        client::Outcome::Failed { status, body } => {
            print_error(&body);
            exit_code_for_status(status)
        }
        client::Outcome::Invalid { body, .. } | client::Outcome::Unreachable { body } => {
            print_error(&body);
            3
        }
    }
}

/// `diagnose`: the admin surface's verdict plus, best-effort, `/v1/status`'s venue health, merged
/// into one value exactly as `channels list` merges the same two surfaces.
///
/// **Exit 0 for any verdict a healthy admin surface produced**, including `tunnel_down`: the report
/// succeeded, and an agent reads `.diagnostics.diagnosis.code` for the answer. Only the admin
/// surface itself failing is an error (3) — there is then nothing to report.
fn run_diagnose(cli: &Cli) -> i32 {
    if cli.template {
        return emit_template(cli, &Endpoint::Diagnose.template());
    }
    let client = match build_http_client() {
        Ok(client) => client,
        Err(msg) => {
            eprintln!("error: {msg}");
            return 1;
        }
    };

    let diagnostics = match client::admin_get(&client, &cli.admin_url, "/admin/diagnostics") {
        client::Outcome::Ok { body } => body,
        client::Outcome::Failed { status, body } => {
            print_error(&body);
            return exit_code_for_status(status);
        }
        client::Outcome::Invalid { body, .. } | client::Outcome::Unreachable { body } => {
            print_error(&body);
            return 3;
        }
    };

    // `/v1` being down is the very condition this command explains, so its failure is silent —
    // a warning here would read as a second fault on top of the one being reported.
    let status = match client::get(&client, &cli.url, "/v1/status", &[]) {
        client::Outcome::Ok { body } => body,
        _ => Value::Null,
    };

    emit(
        cli,
        Endpoint::Diagnose,
        &json!({ "diagnostics": diagnostics, "status": status }),
    )
}

/// Which DoubleZero client verb `connect`/`disconnect` asks the container to run.
#[derive(Clone, Copy)]
enum Attempt {
    Connect,
    Disconnect,
}

impl Attempt {
    fn path(self) -> &'static str {
        match self {
            Attempt::Connect => "/admin/connect",
            Attempt::Disconnect => "/admin/disconnect",
        }
    }

    fn endpoint(self) -> Endpoint {
        match self {
            Attempt::Connect => Endpoint::Connect,
            Attempt::Disconnect => Endpoint::Disconnect,
        }
    }

    /// What the operator is about to do, in the terms it will actually happen in. `disconnect`
    /// takes a working tunnel down, and the prompt says so rather than reading as `connect`'s
    /// mirror image.
    fn preamble(self) -> &'static str {
        match self {
            Attempt::Connect => {
                "This runs `doublezero connect multicast` inside the edge-connect container: it \
                 provisions this host's onchain DoubleZero user and brings the tunnel up. It can \
                 take minutes, and it spends the container's onchain identity."
            }
            Attempt::Disconnect => {
                "This runs `doublezero disconnect multicast` inside the edge-connect container. It \
                 TEARS DOWN this host's DoubleZero tunnel: every market-data feed stops, the /v1 \
                 API deactivates, and nothing is served again until the tunnel is reconnected."
            }
        }
    }
}

/// `connect` / `disconnect`: state what will run, confirm unless `--force`, `POST` it, then watch
/// `/admin/diagnostics` until the attempt finishes.
///
/// Exit code reports the *attempt*, not the request: 0 only when the client verb finished with exit
/// code 0. A timeout is 1 — the run is unfinished, not successful — while `--no-wait` is 0, since
/// there the accepted `202` is all that was asked for.
fn run_attempt(cli: &Cli, args: &AttemptArgs, attempt: Attempt) -> i32 {
    if cli.template {
        return emit_template(cli, &attempt.endpoint().template());
    }
    let client = match build_http_client() {
        Ok(client) => client,
        Err(msg) => {
            eprintln!("error: {msg}");
            return 1;
        }
    };

    println_or_exit(attempt.preamble());
    if !args.force {
        print_or_exit("Type 'yes' to continue: ");
        let mut input = String::new();
        let confirmed = std::io::stdin().read_line(&mut input).is_ok()
            && input.trim().eq_ignore_ascii_case("yes");
        if !confirmed {
            eprintln!("aborted; nothing was run.");
            return 1;
        }
    }

    let accepted = match client::admin_post(&client, &cli.admin_url, attempt.path()) {
        client::Outcome::Ok { body } => body,
        client::Outcome::Failed { status, body } => {
            print_error(&body);
            return exit_code_for_status(status);
        }
        client::Outcome::Invalid { body, .. } | client::Outcome::Unreachable { body } => {
            print_error(&body);
            return 3;
        }
    };

    if args.no_wait {
        let body = json!({"accepted": accepted, "diagnostics": Value::Null, "timed_out": false});
        return emit(cli, attempt.endpoint(), &body);
    }

    let (diagnostics, timed_out) = match watch_attempt(&client, &cli.admin_url, args.timeout) {
        Ok(result) => result,
        Err(body) => {
            print_error(&body);
            return 3;
        }
    };
    let attempt_failed = timed_out || diagnose::attempt_exit_code(&diagnostics) != Some(0);
    let body = json!({"accepted": accepted, "diagnostics": diagnostics, "timed_out": timed_out});
    match emit(cli, attempt.endpoint(), &body) {
        0 if attempt_failed => 1,
        code => code,
    }
}

/// Poll `/admin/diagnostics` until the attempt reports itself finished, or `timeout_secs` elapses
/// (the `bool`). `Err` is an admin-surface error envelope: the attempt has become unobservable from
/// here, which is worth saying immediately rather than spinning out the deadline in silence.
fn watch_attempt(
    client: &reqwest::blocking::Client,
    admin_url: &str,
    timeout_secs: u64,
) -> Result<(Value, bool), Value> {
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    loop {
        // Sleep first: the `202` has only just returned, so the attempt is certainly still running.
        std::thread::sleep(ATTEMPT_POLL_INTERVAL);
        let body = match client::admin_get(client, admin_url, "/admin/diagnostics") {
            client::Outcome::Ok { body } => body,
            client::Outcome::Failed { body, .. }
            | client::Outcome::Invalid { body, .. }
            | client::Outcome::Unreachable { body } => return Err(body),
        };
        if diagnose::attempt_finished(&body) {
            return Ok((body, false));
        }
        if Instant::now() >= deadline {
            return Ok((body, true));
        }
    }
}

/// clap's own `--help`/`--version`/usage errors: the two "not actually an error" kinds print to
/// stdout and exit 0; everything else (an unknown flag, a missing subcommand, ...) is a usage
/// error under this tool's exit-code scheme, which maps it to 1 rather than clap's own default of
/// 2 — see the module-level exit-code table in `--help`.
fn handle_clap_error(e: &clap::Error) -> i32 {
    use clap::error::ErrorKind::{DisplayHelp, DisplayVersion};
    match e.kind() {
        DisplayHelp | DisplayVersion => {
            print_or_exit(&e.to_string());
            0
        }
        _ => {
            eprint!("{e}");
            1
        }
    }
}

/// Print `body` through `--jq` if given, returning the process exit code. Shared by [`emit`] and
/// [`emit_template`] — a jq filter makes sense against either a real response or the `--template`
/// document, since both are just JSON values.
fn print_jq(body: &Value, filter: &str) -> i32 {
    match jq::extract(body, filter) {
        Ok(values) => {
            for v in values {
                println_or_exit(&serde_json::to_string(&v).unwrap_or_default());
            }
            0
        }
        Err(msg) => {
            eprintln!("error: invalid --jq filter: {msg}");
            1
        }
    }
}

/// Print the `--template` document. It is never shaped like a real endpoint response (an empty
/// object, or a small flat map of parameter names to descriptions), so unlike [`emit`] it never
/// goes through `render::render_table` — `--output table` has no meaningful reshaping to do here
/// and would otherwise fail against every endpoint whose real response has required fields this
/// document doesn't carry. `--jq` still applies, since it works against any JSON value.
fn emit_template(cli: &Cli, doc: &Value) -> i32 {
    if let Some(filter) = &cli.jq {
        return print_jq(doc, filter);
    }
    println_or_exit(&serde_json::to_string_pretty(doc).unwrap_or_default());
    0
}

/// Print a successful body per `--jq`/`--output`, returning the process exit code (0, unless
/// rendering itself fails — an unexpected shape mismatch is reported as a usage-adjacent error).
fn emit(cli: &Cli, endpoint: Endpoint, body: &Value) -> i32 {
    if let Some(filter) = &cli.jq {
        return print_jq(body, filter);
    }
    match cli.output {
        OutputFormat::Json => {
            println_or_exit(&serde_json::to_string_pretty(body).unwrap_or_default());
            0
        }
        OutputFormat::Table => match render::render_table(endpoint, body) {
            Ok(text) => {
                println_or_exit(&text);
                0
            }
            Err(msg) => {
                eprintln!("error: could not render table: {msg}");
                1
            }
        },
    }
}

/// Errors are always the raw JSON envelope, pretty-printed, on stderr — never filtered by `--jq`
/// or reshaped by `--output table`. An agent parsing failures wants the same shape every time.
fn print_error(body: &Value) {
    eprintln!("{}", serde_json::to_string_pretty(body).unwrap_or_default());
}

/// HTTP status -> this tool's exit-code scheme (0 ok / 1 usage-validation / 2 not found /
/// 3 unreachable — unreachable is handled separately since it never reaches this function).
/// `404` is the one status that gets its own code; every other non-2xx the API returns today
/// (`400`, `405`, `409`) is a validation/usage problem from the caller's chair, so it lands on 1.
fn exit_code_for_status(status: u16) -> i32 {
    match status {
        200..=299 => 0,
        404 => 2,
        _ => 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn not_found_maps_to_exit_2() {
        assert_eq!(exit_code_for_status(404), 2);
    }

    #[test]
    fn a_validation_status_maps_to_exit_1() {
        assert_eq!(exit_code_for_status(400), 1);
        assert_eq!(exit_code_for_status(409), 1);
        assert_eq!(exit_code_for_status(405), 1);
    }

    #[test]
    fn build_path_encodes_the_product_id() {
        let path = build_path(Endpoint::Ticker, &["LASHAY:EAVE#120.1165".to_string()]).unwrap();
        assert_eq!(path, "/v1/products/LASHAY:EAVE%23120.1165/ticker");
    }

    #[test]
    fn build_path_requires_a_product_id_for_scoped_endpoints() {
        assert!(build_path(Endpoint::Book, &[]).is_err());
    }

    #[test]
    fn build_path_needs_no_id_for_catalog_wide_endpoints() {
        assert_eq!(
            build_path(Endpoint::ProductsList, &[]).unwrap(),
            "/v1/products"
        );
        assert_eq!(build_path(Endpoint::Status, &[]).unwrap(), "/v1/status");
        assert_eq!(
            build_path(Endpoint::BestBidAsk, &[]).unwrap(),
            "/v1/best_bid_ask"
        );
    }

    // -------------------------------------------------------------------------------------------
    // merge_best_bid_ask_params: the CLI-side half of `best_bid_ask` taking a product — bare
    // positional folds into `product_ids`, the faithful `product_ids==A,B` form passes through,
    // and no argument at all leaves the request unfiltered (today's behaviour).
    // -------------------------------------------------------------------------------------------

    #[test]
    fn best_bid_ask_with_no_argument_sends_no_product_ids_param() {
        let out = merge_best_bid_ask_params(vec![], &[]);
        assert!(out.is_empty(), "{out:?}");
    }

    #[test]
    fn best_bid_ask_folds_a_bare_positional_into_product_ids() {
        let out = merge_best_bid_ask_params(vec![], &["HYPERLIQUID:BTC".to_string()]);
        assert_eq!(
            out,
            vec![("product_ids".to_string(), "HYPERLIQUID:BTC".to_string())]
        );
    }

    #[test]
    fn best_bid_ask_passes_through_the_faithful_product_ids_form() {
        let params = vec![("product_ids".to_string(), "A:X,A:Y".to_string())];
        let out = merge_best_bid_ask_params(params, &[]);
        assert_eq!(
            out,
            vec![("product_ids".to_string(), "A:X,A:Y".to_string())]
        );
    }

    /// A bare positional and an explicit `product_ids==...` compose rather than one silently
    /// overriding the other.
    #[test]
    fn best_bid_ask_composes_a_positional_with_an_explicit_product_ids_param() {
        let params = vec![("product_ids".to_string(), "A:X".to_string())];
        let out = merge_best_bid_ask_params(params, &["A:Y".to_string()]);
        assert_eq!(
            out,
            vec![("product_ids".to_string(), "A:X,A:Y".to_string())]
        );
    }

    /// An unrelated param (e.g. a stray `granularity==...`) must survive untouched alongside the
    /// folded `product_ids`.
    #[test]
    fn best_bid_ask_leaves_unrelated_params_untouched() {
        let params = vec![("granularity".to_string(), "ONE_MINUTE".to_string())];
        let out = merge_best_bid_ask_params(params, &["HYPERLIQUID:BTC".to_string()]);
        assert_eq!(
            out,
            vec![
                ("granularity".to_string(), "ONE_MINUTE".to_string()),
                ("product_ids".to_string(), "HYPERLIQUID:BTC".to_string()),
            ]
        );
    }
}
