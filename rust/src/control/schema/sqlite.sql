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

-- One row per (repo, commit). Each output kind (head, files, full) is
-- pending | ready | failed and has its own lease, attempts and retry time,
-- so one row's outputs build at the same time. A kind is building
-- while its lease_until is in the future. A ready output is never
-- rewritten. A kind's attempts counts its claims since admission or the
-- last retry. seq is the admission order: strictly increasing, assigned
-- once at admit, never reused. Claims and the per-repo Full order use it,
-- so same-second admits keep their order.
CREATE TABLE builds (
  repo               TEXT NOT NULL,    -- RepoId::storage_key()
  commit_id          TEXT NOT NULL,
  seq                INTEGER NOT NULL, -- admission order (MAX(seq) + 1 at admit)
  ref_name           TEXT,             -- full ref (refs/heads/main); NULL for SHA-only admits
  parent             TEXT,             -- written by publish(Head)
  created_at         INTEGER NOT NULL, -- epoch seconds
  head_state         TEXT NOT NULL DEFAULT 'pending',
  head_output        TEXT,
  head_error         TEXT,
  head_attempts      INTEGER NOT NULL DEFAULT 0,
  head_retry_after   INTEGER NOT NULL DEFAULT 0,
  head_lease_owner   TEXT,
  head_lease_until   INTEGER,
  files_state        TEXT NOT NULL DEFAULT 'pending',
  files_output       TEXT,
  files_error        TEXT,
  files_attempts     INTEGER NOT NULL DEFAULT 0,
  files_retry_after  INTEGER NOT NULL DEFAULT 0,
  files_lease_owner  TEXT,
  files_lease_until  INTEGER,
  full_state         TEXT NOT NULL DEFAULT 'pending',
  full_output        TEXT,
  full_error         TEXT,
  full_attempts      INTEGER NOT NULL DEFAULT 0,
  full_retry_after   INTEGER NOT NULL DEFAULT 0,
  full_lease_owner   TEXT,
  full_lease_until   INTEGER,
  PRIMARY KEY (repo, commit_id)
);

CREATE UNIQUE INDEX builds_seq ON builds (seq);
CREATE INDEX builds_by_ref ON builds (repo, ref_name, seq);

-- A kind's claims and give-up pass read only rows where that kind is
-- pending: each predicate implies its index's WHERE, so finished rows never
-- enter a scan however many accumulate.
CREATE INDEX builds_head_pending ON builds (seq) WHERE head_state = 'pending';
CREATE INDEX builds_files_pending ON builds (seq) WHERE files_state = 'pending';
CREATE INDEX builds_full_pending ON builds (seq) WHERE full_state = 'pending';
-- A Full's wait for older pending Fulls of its repo.
CREATE INDEX builds_full_order ON builds (repo, seq) WHERE full_state = 'pending';
-- A claim skips a repo that already holds a live lease of the same kind.
CREATE INDEX builds_head_leased ON builds (repo, head_lease_until)
  WHERE head_lease_until IS NOT NULL;
CREATE INDEX builds_files_leased ON builds (repo, files_lease_until)
  WHERE files_lease_until IS NOT NULL;
CREATE INDEX builds_full_leased ON builds (repo, full_lease_until)
  WHERE full_lease_until IS NOT NULL;

-- The repositories whose published outputs name each artifact, one row per
-- (repo, artifact), written in the transaction that publishes the output. A
-- purge removes the repo's rows and queues the artifacts no repo names.
CREATE TABLE artifact_refs (
  repo TEXT NOT NULL,                  -- RepoId::storage_key()
  hash TEXT NOT NULL,
  PRIMARY KEY (repo, hash)
) WITHOUT ROWID;

CREATE INDEX artifact_refs_by_hash ON artifact_refs (hash);

-- Artifacts to delete from storage once due_at passes, if no repository
-- names them then and no build pins them.
CREATE TABLE deletions (
  hash   TEXT PRIMARY KEY,
  due_at INTEGER NOT NULL              -- epoch seconds
) WITHOUT ROWID;

CREATE INDEX deletions_by_due ON deletions (due_at);
