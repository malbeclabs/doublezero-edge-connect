//! End-to-end tests for `doublezero-edge completion <shell>`, driven through the compiled binary.
//!
//! `completion` is local-only — no config file, no server, no network — so packaging can run it
//! at build time. These tests assert on the emitted script's content, not just the exit code: a
//! generator that silently writes nothing (or writes to the wrong stream) would still exit 0.

mod common;

use std::{
    io::Read,
    process::{Command, Stdio},
};

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

/// Piping to a consumer that stops reading early (`| head`, `| less -q`, ...) is completely
/// ordinary for this tool and must exit cleanly rather than panic on the resulting `EPIPE`.
/// Closing our end of the pipe before reading anything guarantees the child's very first write
/// hits a reader that is already gone — reproducing the bug deterministically needs no server,
/// config or network. (A variant that reads a prefix first and closes afterward is racy: on a
/// pipe buffer larger than the ~30KB script, the whole write can complete before the close is
/// even observed, so it sometimes passes without ever exercising the fix.) This test cannot pass
/// vacuously: a bare `println!`/`generate()` reliably panics here, so reverting the fix reliably
/// fails it.
#[test]
fn completion_output_survives_an_early_pipe_close() {
    let mut child = Command::new(bin())
        .args(["completion", "bash"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn doublezero-edge");

    // Drop our read end without reading anything, before the child gets a chance to write.
    drop(child.stdout.take().expect("child stdout"));

    let mut stderr = String::new();
    child
        .stderr
        .take()
        .expect("child stderr")
        .read_to_string(&mut stderr)
        .expect("stderr was not UTF-8");

    let status = child.wait().expect("wait for child");
    assert!(
        status.success(),
        "expected a clean exit after the reader closed early, got {status:?}; stderr: {stderr}"
    );
    assert!(
        !stderr.to_lowercase().contains("panicked"),
        "must not panic on a closed pipe: {stderr}"
    );
}
