//! Contract suite: what ripclone promises, checked against the real
//! `ripclone-server` and `ripclone` binaries. Tests run in parallel; each owns
//! its temp dir, server process, and port. Faults come from outside the
//! product. Spec: docs/internal/rebuild/CONTRACT_TESTS.md.

mod harness;
mod net;
#[path = "../../src/test_tmp.rs"]
mod test_tmp;

mod cli;
mod coalesce;
mod docs;
mod downloads;
mod gitea;
mod incremental;
mod integrations;
mod outputs;
mod pinning;
mod resolve;
mod s3;
mod scheduling;
mod shapes;
mod state;
mod surface;
mod topup;
mod trees;
