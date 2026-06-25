//! Negative tests for queue backend selection from env. Own test file (isolated
//! process) so the global env mutation can't race other tests. One test runs the
//! cases sequentially.

use ripclone::backends::{connect_sql_queue, select_queue};

#[tokio::test]
async fn queue_selection_rejects_bad_config() {
    unsafe {
        std::env::remove_var("RIPCLONE_QUEUE_DB_TOKEN");
    }

    // Unknown backend name.
    unsafe {
        std::env::set_var("RIPCLONE_QUEUE", "bogus");
    }
    assert!(
        select_queue().await.is_err(),
        "unknown RIPCLONE_QUEUE backend should error"
    );

    // libsql with no URL.
    unsafe {
        std::env::set_var("RIPCLONE_QUEUE", "libsql");
        std::env::remove_var("RIPCLONE_QUEUE_DB_URL");
    }
    assert!(
        connect_sql_queue().await.is_err(),
        "libsql without RIPCLONE_QUEUE_DB_URL should error"
    );

    // libsql with a LOCAL path — it's remote-only.
    unsafe {
        std::env::set_var("RIPCLONE_QUEUE_DB_URL", "/tmp/ripclone-queue-test.db");
    }
    assert!(
        connect_sql_queue().await.is_err(),
        "libsql with a local path should error (remote-only)"
    );

    // libsql remote URL but no token.
    unsafe {
        std::env::set_var("RIPCLONE_QUEUE_DB_URL", "libsql://example.turso.io");
        std::env::remove_var("RIPCLONE_QUEUE_DB_TOKEN");
    }
    assert!(
        connect_sql_queue().await.is_err(),
        "libsql remote without RIPCLONE_QUEUE_DB_TOKEN should error"
    );

    unsafe {
        std::env::remove_var("RIPCLONE_QUEUE");
        std::env::remove_var("RIPCLONE_QUEUE_DB_URL");
    }
}
