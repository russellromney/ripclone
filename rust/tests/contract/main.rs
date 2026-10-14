//! Contract suite: what ripclone promises, checked against the real
//! `ripclone-server` and `ripclone` binaries. Tests run in parallel; each owns
//! its temp dir, server process, and port. Faults come from outside the
//! product. Spec: docs/internal/rebuild/CONTRACT_TESTS.md.

mod harness;

mod resolve;
mod shapes;
mod state;
mod surface;
