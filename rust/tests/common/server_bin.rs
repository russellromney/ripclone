//! Start the `ripclone-server` binary on port 0 and learn its address from
//! the "listening on" log line, so no other process can take the port first.

use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};

/// Spawn `command` (a `ripclone-server` invocation without `--host` or
/// `--port`) on 127.0.0.1:0. Returns the child and its base URL once the
/// server answers `/readyz`. stdout is drained in the background.
pub async fn spawn_server_binary(
    mut command: tokio::process::Command,
) -> (tokio::process::Child, String) {
    command
        .args(["--host", "127.0.0.1", "--port", "0"])
        .stdout(std::process::Stdio::piped())
        .kill_on_drop(true);
    let mut child = command.spawn().expect("spawn ripclone-server");
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    let url = tokio::time::timeout(Duration::from_secs(15), async {
        while let Some(line) = lines.next_line().await.unwrap() {
            if let Some(rest) = line.split("listening on http://").nth(1) {
                let addr: String = rest
                    .chars()
                    .take_while(|c| c.is_ascii_digit() || *c == '.' || *c == ':')
                    .collect();
                return format!("http://{addr}");
            }
        }
        panic!("ripclone-server exited before it was listening");
    })
    .await
    .expect("ripclone-server did not report its address within 15s");
    tokio::spawn(async move { while let Ok(Some(_)) = lines.next_line().await {} });
    let ready = format!("{url}/readyz");
    tokio::time::timeout(Duration::from_secs(15), async {
        while !reqwest::get(&ready)
            .await
            .is_ok_and(|response| response.status().is_success())
        {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("ripclone-server did not become ready within 15s");
    (child, url)
}
