//! Contract suite: what ripclone promises, checked against the real
//! `ripclone-server` and `ripclone` binaries. Tests run in parallel; each owns
//! its temp dir, server process, and port. Faults come from outside the
//! product. Spec: docs/internal/rebuild/CONTRACT_TESTS.md.

mod harness;
mod net;

mod coalesce;
mod docs;
mod downloads;
mod gitea;
mod integrations;
mod pinning;
mod resolve;
mod s3;
mod shapes;
mod state;
mod surface;
