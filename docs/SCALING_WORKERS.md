# Running workers at scale

The server resolves refs, admits exact commits, serves artifacts, and owns one
durable SQLite jobs table. It always runs embedded workers against that table.

Additional `ripclone-worker` processes claim and acknowledge through the
authenticated server API. They never connect to SQLite or Turso directly.

## Standalone worker

```bash
export RIPCLONE_QUEUE_API_URL=https://ripclone.example.com
export RIPCLONE_METADATA_REPORT_URL=https://ripclone.example.com/v1/refs
export RIPCLONE_METADATA_JOB_TOKEN=rcjt1...

ripclone-worker \
  --cas-dir /tmp/ripclone-cache \
  --repo-root /tmp/ripclone-repos
```

Mint the bearer with `ripclone mint-worker-token`. Each worker needs its own
scratch `repo_root`; artifact storage may be shared through the ordinary local
or S3-compatible storage settings.

Useful flags:

- `--idle-poll-ms <ms>`: empty-queue poll delay.
- `--idle-exit-secs <seconds>`: exit after continuous empty claims.
- `--max-jobs <count>`: exit after that many settled jobs.
- `--max-size-class <name>`: claim only jobs at or below the configured class.

## Durable claims

Duplicate requests for the same repository, branch, and exact commit coalesce
while queued or claimed. A later commit is a distinct job.

A claim covers Head, Files, and Full publication. An embedded worker releases
its limited foreground slot as soon as Head is published; Full continues in a
background task under the same durable claim. The task heartbeats until it
acknowledges success or failure, and each active heartbeat renews that job's
claim lease.

If the heartbeat or process dies, the server recovers the stale claim after
`RIPCLONE_QUEUE_STALE_SECS`. Empty claim attempts share a coarse stale-sweep
deadline instead of issuing reclaim writes on every poll. Retry and dead-letter
behavior is controlled by:

- `RIPCLONE_QUEUE_MAX_ATTEMPTS`
- `RIPCLONE_QUEUE_RETRY_BACKOFF_MS`
- `RIPCLONE_QUEUE_FAILED_RETENTION_SECS`

Enable API heartbeats with `RIPCLONE_WORKER_HEARTBEAT=queue`. Timing uses
`RIPCLONE_WORKER_HEARTBEAT_INTERVAL_SECS` and
`RIPCLONE_WORKER_HEARTBEAT_TIMEOUT_SECS`.

Local admissions notify idle embedded workers immediately after the SQLite
transaction commits. This notification is only a wake hint: workers always
claim from SQLite first, and a bounded poll remains for restart and recovery.

## Scaling

Run any number of API workers behind an external process manager or compute
platform. Starting and stopping machines is outside ripclone; durable admission
and stale-claim recovery do not depend on how a worker was launched.

The server remains the sole database owner in every topology. Do not distribute
its control path or Turso credentials to workers.
