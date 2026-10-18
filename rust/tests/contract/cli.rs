//! K. The command line: repo arguments, usage errors, provider commands and
//! the login token, as a user types them. I13, I14: what the CLI writes to
//! stderr.

use crate::downloads::noise;
use crate::harness::*;
use crate::net::{Fault, FaultProxy};
use std::io::Read;
use std::os::fd::{FromRawFd, OwnedFd};
use std::process::Stdio;
use std::sync::{Arc, Mutex};

/// K1: `owner/name`, `owner/name.git` and pasted clone URLs name the same
/// repo in every command. A malformed argument or a host with no provider
/// exits 2 and changes nothing.
#[test]
fn k1_repo_arguments_name_one_repo() {
    let world = World::new();
    let origin = world.origin("acme", "k1");
    let tip = origin.commit(&[("a.txt", "a\n")], "A");
    origin.publish();
    let server = world.server(&[]);

    world
        .cli(&server, &["add", "https://github.com/acme/k1", "--wait"])
        .ok();
    world
        .cli(&server, &["add", "git@github.com:acme/k1.git"])
        .ok();
    let out = world.cli(&server, &["sync", "acme/k1.git"]).ok();
    assert_eq!(out, format!("acme/k1 is current at {tip}\n"));
    assert_eq!(world.cli(&server, &["list"]).ok(), "acme/k1\n");
    let target = world.target("k1");
    let out = world
        .cli(
            &server,
            &[
                "clone",
                "ssh://git@github.com/acme/k1.git",
                target.to_str().unwrap(),
                "--depth",
                "1",
            ],
        )
        .ok();
    assert!(out.contains("installed acme/k1"), "{out}");
    assert_eq!(git(&target, &["rev-parse", "HEAD"]), tip);

    for (arg, message) in [
        ("acme", "expected owner/name or a clone URL, got 'acme'"),
        (
            "https://github.com/acme/k1/tree/main",
            "got 'acme/k1/tree/main'",
        ),
        ("https://github.com/", "expected a clone URL with a path"),
        (
            "https://git.example.com/acme/k1",
            "no provider is configured for git.example.com",
        ),
        ("forge:acme/k1", "unknown provider 'forge'"),
    ] {
        for command in ["add", "sync", "clone"] {
            let out = world.cli(&server, &[command, arg]);
            assert_eq!(out.code(), Some(2), "{command} {arg}: {}", out.stderr());
            assert!(
                out.err().contains(message),
                "{command} {arg}: {}",
                out.stderr()
            );
        }
    }
    assert_eq!(world.cli(&server, &["list"]).ok(), "acme/k1\n");

    world
        .cli(&server, &["rm", "https://github.com/acme/k1"])
        .ok();
    let again = world.cli(&server, &["rm", "acme/k1"]);
    assert_eq!(again.code(), Some(5), "{}", again.stderr());
    assert!(again.err().contains("repo not added"));
    assert_eq!(world.cli(&server, &["list"]).ok(), "");
}

/// K2: a bad clone or sync command line exits 2 with a message naming the
/// problem, before it asks the server for anything or creates the target.
#[test]
fn k2_bad_command_lines_exit_2_and_touch_nothing() {
    let world = World::new();
    let origin = world.origin("acme", "k2");
    let tip = origin.commit(&[("a.txt", "a\n")], "A");
    origin.publish();
    let server = world.server(&[]);
    world.cli(&server, &["add", "acme/k2", "--wait"]).ok();
    let queued = server.metric("ripclone_builds_queued_total");
    let target = world.target("k2");
    let dir = target.to_str().unwrap();

    for (args, message) in [
        (vec!["--depth", "3"], "not --depth 3"),
        (vec!["--at", "a b"], "invalid selector `a b`"),
        (vec!["--mode", "foo"], "invalid value 'foo'"),
        (vec!["--bogus"], "unexpected argument '--bogus'"),
        (
            vec!["--at", &tip, "--verify-upstream=always"],
            "cannot verify a non-tip rev",
        ),
        (
            vec!["--mode", "files", "--verify-upstream=always"],
            "cannot verify a files-mode clone",
        ),
        (
            vec!["--verify-upstream=maybe"],
            "expected 'auto', 'always', or 'never'",
        ),
    ] {
        let mut argv = vec!["clone", "acme/k2", dir];
        argv.extend(&args);
        let out = world.cli(&server, &argv);
        assert_eq!(out.code(), Some(2), "{args:?}: {}", out.stderr());
        assert!(out.err().contains(message), "{args:?}: {}", out.stderr());
        assert!(!target.exists(), "{args:?} created the target");
    }
    let out = world.cli(&server, &["sync", "acme/k2", "--at", "a b"]);
    assert_eq!(out.code(), Some(2), "{}", out.stderr());
    assert_eq!(server.metric("ripclone_builds_queued_total"), queued);

    // A `--at` clone under the default `auto` check prints no warning.
    let out = world.cli(&server, &["clone", "acme/k2", dir, "--at", &tip]);
    out.ok();
    assert_eq!(out.stderr(), "", "{}", out.stderr());
}

/// K3: the provider commands manage the CLI's own config. A user adds a
/// private Gitea host, checks it with `provider test`, fixes the token,
/// then adds and clones a repo by pasting its URL.
#[test]
fn k3_provider_commands_manage_the_cli_config() {
    let world = World::new();
    let http = world.serve_origins_over_http();
    http.require_header("authorization", "token k3-secret");
    let origin = world.origin("acme", "k3");
    let tip = origin.commit(&[("a.txt", "a\n")], "A");
    origin.publish();
    world.server_providers(
        http.url(),
        &[("forge", "gitea", Some("k3-secret"), serde_json::json!({}))],
    );
    let server = world.server(&[]);
    let host = http.url();
    let url = format!("{host}/acme/k3.git");

    let list = world.cli(&server, &["provider", "list"]).ok();
    assert!(
        list.lines()
            .any(|l| l.split_whitespace().collect::<Vec<_>>()
                == ["github", "github", "github.com", "none"]),
        "{list}"
    );

    let out = world.cli(&server, &["provider", "add", "forge", "--host", host]);
    assert_eq!(out.code(), Some(2));
    assert!(out.err().contains("--kind is required"));
    let out = world.cli(
        &server,
        &[
            "provider", "add", "forge", "--kind", "gitea", "--host", host, "--token", "wrong",
        ],
    );
    assert_eq!(out.ok(), "added provider 'forge'\n");
    let out = world.cli(&server, &["provider", "test", "forge", "acme/k3"]);
    assert_eq!(out.code(), Some(1));
    assert!(
        out.err().contains(&format!("read {url}")),
        "{}",
        out.stderr()
    );

    // Updating keeps the kind and host.
    let out = world.cli(
        &server,
        &["provider", "add", "forge", "--token", "k3-secret"],
    );
    assert_eq!(out.ok(), "updated provider 'forge'\n");
    let out = world
        .cli(&server, &["provider", "test", "forge", "acme/k3"])
        .ok();
    assert_eq!(out, format!("{url} HEAD is {tip}\n"));
    let out = world.cli(
        &server,
        &["provider", "test", "forge", "acme/k3", "-b", "nope"],
    );
    assert!(out.err().contains("has no ref 'nope'"), "{}", out.stderr());
    let list = world.cli(&server, &["provider", "list"]).ok();
    let clean = host.trim_start_matches("http://");
    assert!(
        list.lines()
            .any(|l| l.split_whitespace().collect::<Vec<_>>()
                == ["forge", "gitea", clean, "configured"]),
        "{list}"
    );

    // The pasted URL picks the provider by host.
    world.cli(&server, &["add", &url, "--wait"]).ok();
    assert_eq!(world.cli(&server, &["list"]).ok(), "forge:acme/k3\n");
    let target = world.target("k3");
    world
        .cli(&server, &["clone", &url, target.to_str().unwrap()])
        .ok();
    assert_eq!(git(&target, &["rev-parse", "HEAD"]), tip);
    assert_eq!(git(&target, &["config", "remote.origin.url"]), url);

    world.cli(&server, &["provider", "rm", "forge"]).ok();
    let out = world.cli(&server, &["provider", "rm", "forge"]);
    assert_eq!(out.code(), Some(1));
    assert!(out.err().contains("not in the CLI's config"));
    let out = world.cli(
        &server,
        &["clone", &url, world.target("k3b").to_str().unwrap()],
    );
    assert_eq!(out.code(), Some(2));
    assert!(
        out.err().contains("no provider is configured"),
        "{}",
        out.stderr()
    );
}

/// K4: `logout` with no saved token says so; `login` against a self-hosted
/// server points at the token variable instead of starting a device flow.
#[test]
fn k4_login_and_logout() {
    let world = World::new();
    let server = world.server(&[]);
    assert_eq!(world.cli(&server, &["logout"]).ok(), "Not logged in.\n");
    let out = world.cli(&server, &["login"]);
    assert_eq!(out.code(), Some(2));
    assert!(
        out.err().contains("set RIPCLONE_SERVER_TOKEN"),
        "{}",
        out.stderr()
    );
}

/// I13: the progress line is for a terminal only. A clone whose stderr is a
/// pipe writes nothing there in any shape, and `--quiet` also drops the
/// "warming" note a pending build prints.
#[test]
fn i13_piped_clone_prints_no_progress() {
    let world = World::new();
    let origin = world.origin("acme", "i13");
    origin.commit(&[("a.txt", "a\n")], "A");
    origin.commit(&[("dir/b.txt", "b\n")], "B");
    origin.publish();
    let mut server = world.server(&[]);
    world.cli(&server, &["add", "acme/i13", "--wait"]).ok();

    let shapes: [&[&str]; 4] = [&["--depth", "1"], &[], &["--mode", "files"], &["--quiet"]];
    for (i, flags) in shapes.into_iter().enumerate() {
        let target = world.target(&format!("i13-{i}"));
        let mut args = vec!["clone", "acme/i13", target.to_str().unwrap()];
        args.extend_from_slice(flags);
        let out = world.cli(&server, &args);
        out.ok();
        assert_eq!(out.stderr(), "", "{flags:?} wrote to stderr");
    }

    server.restart_with(&[("RIPCLONE_FILES_SLOTS", "0"), ("RIPCLONE_FULL_SLOTS", "0")]);
    let c = origin.commit(&[("a.txt", "c\n")], "C");
    origin.publish();
    let target = world.target("i13-pending");
    let args = ["clone", "acme/i13", target.to_str().unwrap(), "--wait"];
    // Polls 50 ms apart run the CLI's 40-poll budget out in about 2 s.
    let poll = [("RIPCLONE_POLL_MS", "50")];
    let err = world.cli_env(&server, &args, &poll).err();
    assert!(err.contains("warming"), "{err}");
    let quiet = [&args[..], &["--quiet"]].concat();
    let err = world.cli_env(&server, &quiet, &poll).err();
    assert!(err.contains("pending") && err.contains(&c), "{err}");
    assert!(!err.contains("warming"), "{err}");
    assert!(!err.contains('\r'), "{err:?}");
}

/// The CLI's stderr on a pseudo-terminal or a pipe. A thread collects what
/// it shows as it is written.
struct Terminal {
    seen: Arc<Mutex<Vec<u8>>>,
}

impl Terminal {
    /// A terminal `columns` wide, and the end to hand the CLI as its stderr.
    fn open(columns: u16) -> (Self, Stdio) {
        let (mut master, mut slave) = (0, 0);
        let mut size = libc::winsize {
            ws_row: 24,
            ws_col: columns,
            ws_xpixel: 0,
            ws_ypixel: 0,
        };
        let opened = unsafe {
            libc::openpty(
                &mut master,
                &mut slave,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                // macOS takes `*mut winsize`, Linux `*const`; a raw `*mut`
                // suits both.
                &raw mut size,
            )
        };
        assert_eq!(opened, 0, "openpty: {}", std::io::Error::last_os_error());
        Self::collect(master, slave)
    }

    /// Not a terminal: a pipe, read as the CLI writes it.
    fn pipe() -> (Self, Stdio) {
        let mut fds = [0; 2];
        let opened = unsafe { libc::pipe(fds.as_mut_ptr()) };
        assert_eq!(opened, 0, "pipe: {}", std::io::Error::last_os_error());
        Self::collect(fds[0], fds[1])
    }

    /// Read `reader` on a thread; `writer` goes to the CLI.
    fn collect(reader: i32, writer: i32) -> (Self, Stdio) {
        let mut master = unsafe { std::fs::File::from_raw_fd(reader) };
        let slave = unsafe { OwnedFd::from_raw_fd(writer) };
        let seen = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&seen);
        // Ends when the last writer closes the terminal (EOF, or EIO on
        // Linux).
        std::thread::spawn(move || {
            let mut buf = [0u8; 4096];
            while let Ok(n @ 1..) = master.read(&mut buf) {
                sink.lock().unwrap().extend_from_slice(&buf[..n]);
            }
        });
        (Self { seen }, Stdio::from(slave))
    }

    fn text(&self) -> String {
        String::from_utf8_lossy(&self.seen.lock().unwrap()).into_owned()
    }

    fn wait_for(&self, what: &str) {
        wait_for(&format!("{what:?} on the terminal"), || {
            self.text().contains(what).then_some(())
        });
    }

    /// Everything shown, once the last line drawn has been blanked.
    fn cleared(&self) -> String {
        wait_for("the progress line to be cleared", || {
            let text = self.text();
            let last = text.rsplit('\r').next().unwrap_or_default();
            (text.contains('\r') && last.is_empty()).then_some(text)
        })
    }
}

/// I14: on a terminal, a clone redraws one line with its phase and bytes,
/// then blanks it before printing the result on stdout. A top-up shows the
/// fetch's phase and seconds instead of the base's bytes.
#[test]
fn i14_terminal_clone_draws_then_clears_progress() {
    let world = World::new();
    let origin = world.origin("acme", "i14");
    std::fs::write(origin.work.join("blob.bin"), noise(14, 4 << 20)).unwrap();
    origin.commit(&[("a.txt", "a\n")], "A");
    origin.publish();
    let server = world.server(&[]);
    world.cli(&server, &["add", "acme/i14", "--wait"]).ok();

    // Holding the large pack keeps the clone downloading until the line is
    // on the terminal.
    let proxy = FaultProxy::start(&server);
    proxy.fault_artifacts(Fault::Hold, 1 << 20, 1);
    let (terminal, stderr) = Terminal::open(200);
    let target = world.target("i14");
    let args = ["clone", "acme/i14", target.to_str().unwrap()];
    let cli = world.cli_spawn_stderr(&proxy, &args, &[], stderr);
    terminal.wait_for("ripclone: downloading ");
    proxy.release();
    let out = cli.wait();
    out.ok();

    assert_eq!(
        out.stdout(),
        format!("installed acme/i14 into {}\n", target.display())
    );
    let shown = terminal.cleared();
    assert!(
        shown.contains(" MiB (") && shown.contains("%)"),
        "{shown:?}"
    );
    assert!(!shown.contains('\x1b'), "escape codes: {shown:?}");
}

#[test]
fn i14_terminal_top_up_shows_its_phase_and_seconds() {
    let world = World::new();
    let http = world.serve_origins_over_http();
    let origin = world.origin("acme", "i14t");
    origin.commit(&[("a.txt", "a\n")], "A");
    origin.publish();
    let gh = [("gh", "github", None, serde_json::json!({}))];
    world.server_providers(http.url(), &gh);
    world.cli_providers(http.url(), &gh);
    let mut server = world.server(&[]);
    world.cli(&server, &["add", "gh:acme/i14t", "--wait"]).ok();
    server.restart_with(&[("RIPCLONE_FILES_SLOTS", "0"), ("RIPCLONE_FULL_SLOTS", "0")]);
    let b = origin.commit(&[("a.txt", "b\n")], "B");
    origin.publish();

    // The origin parks every fetch, the top-up's included, until the line
    // has counted a second.
    http.hold_fetches();
    let (terminal, stderr) = Terminal::open(200);
    let target = world.target("i14t");
    let args = ["clone", "gh:acme/i14t", target.to_str().unwrap()];
    let cli = world.cli_spawn_stderr(&server, &args, &[], stderr);
    let phase = format!("ripclone: fetching {} from upstream", &b[..7]);
    terminal.wait_for(&format!("{phase} (1s)"));
    http.release_fetches();
    cli.wait().ok();

    assert_eq!(git(&target, &["rev-parse", "HEAD"]), b);
    let shown = terminal.cleared();
    let fetch_lines = shown.split('\r').filter(|line| line.starts_with(&phase));
    for line in fetch_lines {
        assert!(!line.contains("MiB") && !line.contains('%'), "{line:?}");
    }
}

/// I14: `add --wait` on a terminal draws the commit's output states while it
/// waits, cut to the terminal's width, and clears them before stdout.
#[test]
fn i14_terminal_add_wait_shows_output_states_within_its_width() {
    let world = World::new();
    let http = world.serve_origins_over_http();
    let origin = world.origin("acme", "i14w");
    let a = origin.commit(&[("a.txt", "a\n")], "A");
    origin.publish();
    let gh = [("gh", "github", None, serde_json::json!({}))];
    world.server_providers(http.url(), &gh);
    world.cli_providers(http.url(), &gh);
    let server = world.server(&[]);

    // The origin parks the build's fetch until the line is on the terminal.
    http.hold_fetches();
    let (terminal, stderr) = Terminal::open(60);
    let cli = world.cli_spawn_stderr(&server, &["add", "gh:acme/i14w", "--wait"], &[], stderr);
    terminal.wait_for(&format!("ripclone: waiting for {}: Head pending", &a[..7]));
    http.release_fetches();
    let out = cli.wait();
    out.ok();

    assert_eq!(out.stdout(), format!("added acme/i14w at {a}\n"));
    let shown = terminal.cleared();
    for line in shown.split('\r') {
        assert!(
            line.chars().count() < 60,
            "wider than the terminal: {line:?}"
        );
    }
}

/// I14: `--progress` draws the line on a pipe too.
#[test]
fn i14_progress_flag_draws_on_a_pipe() {
    let world = World::new();
    let origin = world.origin("acme", "i14p");
    std::fs::write(origin.work.join("blob.bin"), noise(15, 4 << 20)).unwrap();
    origin.commit(&[("a.txt", "a\n")], "A");
    origin.publish();
    let server = world.server(&[]);
    world.cli(&server, &["add", "acme/i14p", "--wait"]).ok();

    let err = world
        .cli(
            &server,
            &["clone", "acme/i14p", "x", "--progress", "--quiet"],
        )
        .err();
    assert!(err.contains("cannot be used with"), "{err}");

    let proxy = FaultProxy::start(&server);
    proxy.fault_artifacts(Fault::Hold, 1 << 20, 1);
    let (pipe, stderr) = Terminal::pipe();
    let target = world.target("i14p");
    let args = ["clone", "acme/i14p", target.to_str().unwrap(), "--progress"];
    let cli = world.cli_spawn_stderr(&proxy, &args, &[], stderr);
    pipe.wait_for("\rripclone: downloading ");
    proxy.release();
    cli.wait().ok();
    pipe.cleared();
}
