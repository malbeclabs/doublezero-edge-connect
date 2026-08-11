//! End-to-end tests for `doublezero-edge completion <shell>`, driven through the compiled binary.
//!
//! `completion` is local-only — no config file, no server, no network — so packaging can run it
//! at build time. These tests assert on the emitted script's content, not just the exit code: a
//! generator that silently writes nothing (or writes to the wrong stream) would still exit 0.

mod common;

use std::process::Command;

use common::bin;

struct Run {
    status: i32,
    stdout: String,
    stderr: String,
}

fn run(args: &[&str]) -> Run {
    let out = Command::new(bin())
        .args(args)
        .output()
        .expect("spawn doublezero-edge");
    Run {
        status: out
            .status
            .code()
            .expect("process exited via a signal, not a status code"),
        stdout: String::from_utf8(out.stdout).expect("stdout was not UTF-8"),
        stderr: String::from_utf8(out.stderr).expect("stderr was not UTF-8"),
    }
}

#[test]
fn bash_completion_is_nonempty_and_names_the_binary() {
    let r = run(&["completion", "bash"]);
    assert_eq!(r.status, 0, "stderr: {}", r.stderr);
    assert!(!r.stdout.trim().is_empty(), "stdout was empty");
    assert!(
        r.stdout.contains("doublezero-edge"),
        "bash completion script must name the binary: {}",
        r.stdout
    );
    // bash completion scripts register via `complete`.
    assert!(r.stdout.contains("complete "), "{}", r.stdout);
}

#[test]
fn zsh_completion_is_nonempty_and_names_the_binary() {
    let r = run(&["completion", "zsh"]);
    assert_eq!(r.status, 0, "stderr: {}", r.stderr);
    assert!(!r.stdout.trim().is_empty(), "stdout was empty");
    assert!(
        r.stdout.contains("doublezero-edge"),
        "zsh completion script must name the binary: {}",
        r.stdout
    );
    // zsh completion scripts declare a `#compdef` function.
    assert!(r.stdout.contains("#compdef"), "{}", r.stdout);
}

#[test]
fn fish_completion_is_nonempty_and_names_the_binary() {
    let r = run(&["completion", "fish"]);
    assert_eq!(r.status, 0, "stderr: {}", r.stderr);
    assert!(!r.stdout.trim().is_empty(), "stdout was empty");
    assert!(
        r.stdout.contains("doublezero-edge"),
        "fish completion script must name the binary: {}",
        r.stdout
    );
    // fish completion scripts register via `complete -c <bin>`.
    assert!(
        r.stdout.contains("complete -c doublezero-edge"),
        "{}",
        r.stdout
    );
}

/// An unrecognized shell is a clap usage error (invalid value for the `value_enum`), never a
/// network call or a panic.
#[test]
fn an_unknown_shell_exits_nonzero() {
    let r = run(&["completion", "tcsh"]);
    assert_ne!(r.status, 0, "stdout: {} stderr: {}", r.stdout, r.stderr);
    assert!(
        r.stdout.trim().is_empty(),
        "a usage error must not write a completion script to stdout: {}",
        r.stdout
    );
}

/// `completion` must never build an HTTP client or touch the network — pointing `--url` at a dead
/// address must not change the outcome (this would hang/fail as unreachable if it did).
#[test]
fn completion_never_touches_the_network() {
    let r = run(&["--url", "http://127.0.0.1:1", "completion", "bash"]);
    assert_eq!(r.status, 0, "stderr: {}", r.stderr);
    assert!(r.stdout.contains("doublezero-edge"), "{}", r.stdout);
}
