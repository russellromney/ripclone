//! Read upstream refs with git's protocol v2 `ls-refs` as one HTTP POST on a
//! kept-alive connection. `git ls-remote` starts a process, opens a new TLS
//! connection and sends two requests; on GitHub that is about 0.2 s, and this
//! is about 0.05 s once the connection is open.
//!
//! Anything this path does not expect (no HTTP URL, a status other than 200,
//! a redirect, a malformed answer, a TLS or connection failure) returns
//! `None`, and the caller runs `git ls-remote`, which reports errors as it
//! always has and can ask a credential helper.

use std::sync::OnceLock;
use std::time::Duration;

const AGENT: &str = concat!("git/ripclone-", env!("CARGO_PKG_VERSION"));

/// The most an answer may hold. With `ref-prefix` an answer is a few lines.
const MAX_ANSWER: usize = 1 << 20;

/// One client for every read, so the connection to each host stays open
/// between reads. HTTP/2 pings keep an idle connection alive.
fn client() -> Option<&'static reqwest::Client> {
    static CLIENT: OnceLock<Option<reqwest::Client>> = OnceLock::new();
    CLIENT
        .get_or_init(|| {
            reqwest::Client::builder()
                .user_agent(AGENT)
                // A credential header must never follow a redirect to
                // another host; git's fallback refuses them too.
                .redirect(reqwest::redirect::Policy::none())
                .pool_idle_timeout(Duration::from_secs(300))
                .http2_keep_alive_interval(Duration::from_secs(30))
                .http2_keep_alive_timeout(Duration::from_secs(10))
                .http2_keep_alive_while_idle(true)
                .build()
                .ok()
        })
        .as_ref()
}

/// The refs of `url` whose names start with one of `prefixes`, as the lines
/// `git ls-remote --symref` prints: `ref: <target>\t<name>` for a symbolic
/// ref, `<oid>\t<name>`, and `<oid>\t<name>^{}` for a peeled tag. `None`
/// when the caller should run `git ls-remote` instead.
pub(crate) async fn ls_refs(
    url: &str,
    auth_header: Option<&(String, String)>,
    prefixes: &[String],
    timeout: Duration,
) -> Option<Vec<u8>> {
    if !(url.starts_with("https://") || url.starts_with("http://")) {
        return None;
    }
    let endpoint = format!("{}/git-upload-pack", url.trim_end_matches('/'));
    let mut request = client()?
        .post(&endpoint)
        .timeout(timeout)
        .header("Git-Protocol", "version=2")
        .header("Content-Type", "application/x-git-upload-pack-request")
        .header("Accept", "application/x-git-upload-pack-result")
        .body(request_body(prefixes));
    if let Some((name, value)) = auth_header {
        request = request.header(name.as_str(), value.as_str());
    }
    let answer = async {
        let mut response = request.send().await.ok()?;
        if response.status() != reqwest::StatusCode::OK {
            return None;
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.ok()? {
            body.extend_from_slice(&chunk);
            if body.len() > MAX_ANSWER {
                return None;
            }
        }
        Some(body)
    };
    let body = answer.await;
    let lines = body.as_deref().and_then(ls_remote_lines);
    if lines.is_none() {
        tracing::debug!(%url, "ls-refs over HTTP gave no usable answer; running git ls-remote");
    }
    lines
}

/// One pkt-line: four hex digits of length, then the payload.
fn pkt_line(out: &mut Vec<u8>, payload: &str) {
    out.extend_from_slice(format!("{:04x}", payload.len() + 4).as_bytes());
    out.extend_from_slice(payload.as_bytes());
}

/// The `ls-refs` command with symbolic refs and peeled tags, for refs under
/// `prefixes`. A `^{}` suffix (a peeled tag in `ls-remote` terms) is dropped.
fn request_body(prefixes: &[String]) -> Vec<u8> {
    let mut body = Vec::new();
    pkt_line(&mut body, "command=ls-refs\n");
    pkt_line(&mut body, &format!("agent={AGENT}\n"));
    pkt_line(&mut body, "object-format=sha1\n");
    body.extend_from_slice(b"0001");
    pkt_line(&mut body, "peel\n");
    pkt_line(&mut body, "symrefs\n");
    let mut seen = Vec::new();
    for prefix in prefixes {
        let prefix = prefix.trim_end_matches("^{}");
        if !seen.contains(&prefix) {
            seen.push(prefix);
            pkt_line(&mut body, &format!("ref-prefix {prefix}\n"));
        }
    }
    body.extend_from_slice(b"0000");
    body
}

/// An `ls-refs` answer as `git ls-remote --symref` lines, or `None` when it is
/// not one: each pkt-line is `<oid> <name>` with optional
/// `symref-target:<ref>` and `peeled:<oid>` attributes, ending in a flush.
fn ls_remote_lines(answer: &[u8]) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    let mut rest = answer;
    loop {
        let len = std::str::from_utf8(rest.get(..4)?).ok()?;
        let len = usize::from_str_radix(len, 16).ok()?;
        if len == 0 {
            return rest[4..].is_empty().then_some(out);
        }
        let line = std::str::from_utf8(rest.get(4..len)?).ok()?;
        rest = &rest[len..];
        let mut fields = line.trim_end_matches('\n').split(' ');
        let (oid, name) = (fields.next()?, fields.next()?);
        if crate::validation::validate_object_id(oid).is_err() || name.is_empty() {
            return None;
        }
        let mut peeled = None;
        for attribute in fields {
            if let Some(target) = attribute.strip_prefix("symref-target:") {
                out.extend_from_slice(format!("ref: {target}\t{name}\n").as_bytes());
            } else if let Some(oid) = attribute.strip_prefix("peeled:") {
                crate::validation::validate_object_id(oid).ok()?;
                peeled = Some(oid);
            }
        }
        out.extend_from_slice(format!("{oid}\t{name}\n").as_bytes());
        if let Some(peeled) = peeled {
            out.extend_from_slice(format!("{peeled}\t{name}^{{}}\n").as_bytes());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const A: &str = "dbfe46bd1d068a7e5b41086298ce23052c8d521f";
    const B: &str = "0123456789abcdef0123456789abcdef01234567";

    fn answer(lines: &[&str]) -> Vec<u8> {
        let mut out = Vec::new();
        for line in lines {
            pkt_line(&mut out, &format!("{line}\n"));
        }
        out.extend_from_slice(b"0000");
        out
    }

    #[test]
    fn the_request_names_each_prefix_once() {
        let prefixes = [
            "refs/heads/v1".to_string(),
            "refs/tags/v1^{}".to_string(),
            "refs/tags/v1".to_string(),
        ];
        let body = String::from_utf8(request_body(&prefixes)).unwrap();
        assert!(body.starts_with("0014command=ls-refs\n"), "{body}");
        assert!(body.contains("0017object-format=sha1\n0001"), "{body}");
        assert!(body.ends_with("001dref-prefix refs/heads/v1\n001cref-prefix refs/tags/v1\n0000"));
    }

    #[test]
    fn an_answer_reads_as_ls_remote_lines() {
        let lines = ls_remote_lines(&answer(&[
            &format!("{A} HEAD symref-target:refs/heads/master"),
            &format!("{A} refs/heads/master"),
            &format!("{B} refs/tags/v1 peeled:{A}"),
        ]))
        .unwrap();
        assert_eq!(
            String::from_utf8(lines).unwrap(),
            format!(
                "ref: refs/heads/master\tHEAD\n{A}\tHEAD\n{A}\trefs/heads/master\n\
                 {B}\trefs/tags/v1\n{A}\trefs/tags/v1^{{}}\n"
            )
        );
        assert_eq!(ls_remote_lines(b"0000").unwrap(), b"");
    }

    /// A server that accepts the connection and never answers is given up on
    /// at the timeout, so `git ls-remote` still gets its turn.
    #[tokio::test]
    async fn a_silent_server_is_given_up_on() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}/acme/repo.git", listener.local_addr().unwrap());
        let _held = std::thread::spawn(move || listener.accept().map(|(stream, _)| stream));
        let started = std::time::Instant::now();
        let answer = ls_refs(
            &url,
            None,
            &["HEAD".to_string()],
            Duration::from_millis(300),
        )
        .await;
        assert_eq!(answer, None);
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "{:?}",
            started.elapsed()
        );
    }

    #[test]
    fn anything_else_is_not_an_answer() {
        for bad in [
            b"".to_vec(),
            b"00".to_vec(),
            b"zzzz".to_vec(),
            // An error packet.
            answer(&["ERR upload-pack: not our ref"]),
            answer(&["deadbeef refs/heads/main"]),
            answer(&[&format!("{A} refs/heads/main peeled:nope")]),
            // A v0 advertisement.
            answer(&["# service=git-upload-pack"]),
            // No flush, or bytes after it.
            answer(&[&format!("{A} refs/heads/main")])[..50].to_vec(),
            [answer(&[]), b"0000".to_vec()].concat(),
        ] {
            assert_eq!(
                ls_remote_lines(&bad),
                None,
                "{:?}",
                String::from_utf8_lossy(&bad)
            );
        }
    }
}
