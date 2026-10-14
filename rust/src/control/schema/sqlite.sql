-- Control store schema for SQLite. One file per backend; no migrations. A
-- database whose schema_version row is missing or different is refused.
-- The version row itself is written by the store (SCHEMA_VERSION in sqlite.rs).

CREATE TABLE schema_version (
  id      INTEGER PRIMARY KEY CHECK (id = 1),
  version INTEGER NOT NULL
);

CREATE TABLE added_repos (
  repo     TEXT PRIMARY KEY,           -- RepoId::storage_key()
  added_at INTEGER NOT NULL            -- epoch seconds
);

CREATE TABLE repo_config (
  repo TEXT PRIMARY KEY,               -- RepoId::storage_key()
  data TEXT NOT NULL                   -- serde JSON of RepoConfig
);

-- One row per (repo, commit). Each output kind is pending | ready | failed;
-- "building" means lease_until is in the future. A ready output is never
-- rewritten. attempts counts claims in the current phase (Head, Files, then
-- Full); publishing Head and settling Files reset it.
CREATE TABLE builds (
  repo         TEXT NOT NULL,          -- RepoId::storage_key()
  commit_id    TEXT NOT NULL,
  ref_name     TEXT,                   -- full ref (refs/heads/main); NULL for SHA-only admits
  parent       TEXT,                   -- written by publish(Head)
  created_at   INTEGER NOT NULL,       -- epoch seconds
  head_state   TEXT NOT NULL DEFAULT 'pending',
  head_output  TEXT,
  head_error   TEXT,
  files_state  TEXT NOT NULL DEFAULT 'pending',
  files_output TEXT,
  files_error  TEXT,
  full_state   TEXT NOT NULL DEFAULT 'pending',
  full_output  TEXT,
  full_error   TEXT,
  attempts     INTEGER NOT NULL DEFAULT 0,
  lease_owner  TEXT,
  lease_until  INTEGER,
  retry_after  INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (repo, commit_id)
);

CREATE INDEX builds_claim ON builds (head_state, retry_after, created_at);
CREATE INDEX builds_by_ref ON builds (repo, ref_name, created_at);
