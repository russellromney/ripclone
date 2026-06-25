//! `config.toml` drives the server-side backend selection, and `RIPCLONE_*` env
//! vars override it. Own test file (separate process) because it mutates `HOME`
//! and the backend config is cached once per process.

use ripclone::backends;

#[test]
fn config_toml_drives_queue_selection_env_overrides() {
    let home = tempfile::tempdir().expect("home dir");
    let cfg_dir = home.path().join(".config").join("ripclone");
    std::fs::create_dir_all(&cfg_dir).unwrap();
    std::fs::write(
        cfg_dir.join("config.toml"),
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
        std::env::set_var("HOME", home.path());
        std::env::remove_var("RIPCLONE_QUEUE");
        std::env::remove_var("RIPCLONE_QUEUE_DB_URL");
    }

    // No env set → the backend comes from config.toml. (This first call also
    // caches the file config for the rest of the process.)
    assert_eq!(backends::queue_kind(), "sqlite");
    assert_eq!(
        backends::queue_db_url().unwrap(),
        "/tmp/ripclone-config-test-queue.db"
    );

    // Env always wins over the file: the URL still comes from config, but the
    // backend selection now reflects the env var.
    unsafe { std::env::set_var("RIPCLONE_QUEUE", "postgres") };
    assert_eq!(backends::queue_kind(), "postgres");

    unsafe {
        std::env::set_var("RIPCLONE_QUEUE_DB_URL", "postgres://override/db");
    }
    assert_eq!(backends::queue_db_url().unwrap(), "postgres://override/db");

    unsafe {
        std::env::remove_var("RIPCLONE_QUEUE");
        std::env::remove_var("RIPCLONE_QUEUE_DB_URL");
    }
}
