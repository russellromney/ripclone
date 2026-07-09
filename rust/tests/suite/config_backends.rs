//! `config.toml` drives the server-side backend selection, `RIPCLONE_CONFIG`
//! points at an explicit file, and `RIPCLONE_*` env vars override it. Own test
//! file (separate process) because it mutates global env and the backend config
//! is cached once per process.

use ripclone::backends;

#[test]
fn explicit_config_drives_queue_selection_env_overrides() {
    let dir = tempfile::tempdir().expect("config dir");
    let cfg_path = dir.path().join("ripclone.toml");
    std::fs::write(
        &cfg_path,
        r#"
[queue]
backend = "sqlite"
url = "/tmp/ripclone-config-test-queue.db"

[metadata]
backend = "sqlite"
url = "/tmp/ripclone-config-test-meta.db"
"#,
    )
    .unwrap();

    unsafe {
        // RIPCLONE_CONFIG points the server at an explicit file (no $HOME dance).
        std::env::set_var("RIPCLONE_CONFIG", &cfg_path);
        std::env::remove_var("RIPCLONE_QUEUE");
        std::env::remove_var("RIPCLONE_QUEUE_DB_URL");
    }

    // No env set → the backend comes from the config file. (This first call also
    // caches the global config for the rest of the process.)
    assert_eq!(backends::queue_kind(), "sqlite");
    assert_eq!(
        backends::queue_db_url().unwrap(),
        "/tmp/ripclone-config-test-queue.db"
    );

    // Env always wins over the file.
    unsafe { std::env::set_var("RIPCLONE_QUEUE", "postgres") };
    assert_eq!(backends::queue_kind(), "postgres");

    unsafe {
        std::env::set_var("RIPCLONE_QUEUE_DB_URL", "postgres://override/db");
    }
    assert_eq!(backends::queue_db_url().unwrap(), "postgres://override/db");

    unsafe {
        std::env::remove_var("RIPCLONE_QUEUE");
        std::env::remove_var("RIPCLONE_QUEUE_DB_URL");
        std::env::remove_var("RIPCLONE_CONFIG");
    }
}
