//! `doublezero-edge`: an agent-facing, READ-ONLY CLI over a running `doublezero-edge-connect`
//! container's `/v1` HTTP market-data API. Command surface and JSON envelopes are modelled on the
//! Coinbase Advanced Trade CLI (`key==value` query parameters, `--jq`, `--template`) so an agent
//! already fluent in that tool needs no new vocabulary here.
//!
//! There is no order-placement or mutation path anywhere in `edge-connect` for this tool to reach
//! — every command is a `GET` — so unlike the tool it emulates, no command here ever needs a
//! confirmation prompt.

use std::time::Duration;

use clap::{Parser, Subcommand};
use doublezero_edge::{client, endpoint::Endpoint, jq, params, render};
use serde_json::Value;

/// doublezero-edge: an agent-facing, READ-ONLY CLI over a running edge-connect container's /v1
/// market-data API.
#[derive(Parser)]
#[command(
    name = "doublezero-edge",
    version,
    about = "Read-only market-data client for a running doublezero-edge-connect container.",
    long_about = "doublezero-edge queries a running doublezero-edge-connect container's /v1 HTTP \
API for market data: products, tickers, candles, order books, best bid/ask, and feed health.\n\n\
This tool is READ-ONLY. Every command is a GET against the API; there is no order-placement or \
mutation path anywhere in edge-connect for it to reach. That means no command here ever needs a \
confirmation prompt — a real difference from the Coinbase Advanced Trade CLI this tool's surface \
is modelled on, where blast-radius containment (accidentally placing a real order) drives much of \
the design.\n\n\
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
}

#[derive(Subcommand)]
enum ProductsCommand {
    /// List every known product.
    List {
        #[arg(num_args = 0.., value_name = "ARGS")]
        args: Vec<String>,
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
    /// Best bid/ask across products.
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
            ProductsCommand::List { args } => (Endpoint::ProductsList, args.clone()),
            ProductsCommand::Get { args } => (Endpoint::ProductGet, args.clone()),
            ProductsCommand::Ticker { args } => (Endpoint::Ticker, args.clone()),
            ProductsCommand::Candles { args } => (Endpoint::Candles, args.clone()),
            ProductsCommand::Book { args } => (Endpoint::Book, args.clone()),
            ProductsCommand::BestBidAsk { args } => (Endpoint::BestBidAsk, args.clone()),
        },
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
    })
}

fn main() {
    std::process::exit(run());
}

fn run() -> i32 {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(e) => return handle_clap_error(&e),
    };

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

    let client = match reqwest::blocking::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(10))
        .build()
    {
        Ok(client) => client,
        Err(e) => {
            eprintln!("error: failed to build HTTP client: {e}");
            return 1;
        }
    };

    match client::get(&client, &cli.url, &path, &params) {
        client::Outcome::Ok { body } => emit(&cli, endpoint, &body),
        client::Outcome::Failed { status, body } => {
            print_error(&body);
            exit_code_for_status(status)
        }
        client::Outcome::Unreachable { body } => {
            print_error(&body);
            3
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
            print!("{e}");
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
                println!("{}", serde_json::to_string(&v).unwrap_or_default());
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
    println!("{}", serde_json::to_string_pretty(doc).unwrap_or_default());
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
            println!("{}", serde_json::to_string_pretty(body).unwrap_or_default());
            0
        }
        OutputFormat::Table => match render::render_table(endpoint, body) {
            Ok(text) => {
                println!("{text}");
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
}
