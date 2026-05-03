# Zeplicator

A ZFS replication manager for multi-node chains. Each node runs `zep` from cron;
the current master creates timestamped snapshots for each configured label, sends them
incrementally down the chain with `mbuffer`+`zstd` compression, and cascades to the
next hop automatically. Non-master nodes silently rotate old snapshots until they
become master.

## Features

- **Multi-label replication** — replicate several snapshot labels (`min1`, `hour1`,
  `day1`, …) in one invocation. Each label has its own interval that must elapse
  before a new snapshot is created (unless `--now` is used).
- **Cascading chain** — downstream nodes cascade replication to the next hop
  immediately after a successful local receive. The master waits for the cascade to
  reach the sink before marking the snapshot as shipped.
- **Time-interval gating** — labels encode their interval in the name (`min5` → 300s,
  `hour2` → 7200s, `day1` → 86400s, etc.). Cron calls `zep` every minute and only
  creates snapshots whose interval has elapsed.
- **GUID-based consistency** — all common-snapshot discovery uses ZFS GUIDs, making
  the system immune to snapshot renames or clock drift between nodes.
- **Split-brain detection** — if the destination has been modified locally, the
  pipeline aborts before overwriting data. A critical SMTP alert fires and the
  `zep:error:split-brain` property is set.
- **Donor discovery & recovery** — when a downstream node's common ground is lost,
  the next upstream node searches the entire chain for a suitable donor to re-
  establish the replication path without a full re-init.
- **Common-ground preservation** — the GUID of the last snapshot shipped to each
  downstream node is stored in `zep:node:<alias>:last_snapshot`. Rotation on the
  master and sink preserves these snapshots so a returning node can always find
  common ground, avoiding unnecessary donor searches.
- **Resilience mode** — with `zep:policy=resilience`, unreachable or split-brain
  nodes are skipped and the chain continues. Exit code 3 signals partial success.
- **Graduated retention** — per-role retention counts (`master`, `middle`, `sink`)
  with shipped-aware purge: snapshots already propagated to downstream nodes are
  pruned first.
- **Chain promotion** — any node can be promoted to master with automatic common-
  snapshot consensus and property propagation across the chain.
- **Config sync** — all `zep:*` properties are automatically propagated from the
  master to every downstream node during replication.
- **SMTP alerts** — critical (split-brain, heartbeat lost), warning (unreachable
  node, resume failure), and info (init success, promotion, donor recovery) alerts
  with per-dataset rate-limiting.
- **Dry-run mode** — simulate the full chain without touching ZFS or sending data.
- **Status dashboard** — `--status` shows chain health, pool capacity, IO stats,
  snapshot ages vs heartbeat thresholds, and retention percentages.
- **Self-contained binary** — `make` assembles libraries, the orchestrator, and the
  `zpipe` C binary into a single `build/zep` script.

## Dependencies

All nodes need:

- `zfsutils-linux`
- `openssh-server` / `openssh-client`
- `mbuffer`
- `zstd`
- `curl` (for SMTP alerts)

## Installation

```bash
git clone https://github.com/arg7/zeplicator.git
cd zeplicator
make
sudo make install         # installs to /usr/local/bin
```

Build artifacts are in `build/`:
- `zep` — assembled standalone script
- `zpipe` — compiled C pipe monitor
- `alertcon` — debug SMTP server (development only)

## Quick Start

### 1. Configure the master

```bash
zep --fs tank/data --config \
  chain=node1,node2,node3          \
  node:node1:fs=tank/data           \
  node:node1:fqdn=10.0.0.1          \
  node:node2:fs=tank/data           \
  node:node2:fqdn=10.0.0.2          \
  node:node3:fs=tank/data           \
  node:node3:fqdn=10.0.0.3          \
  user=repluser                     \
  role:master:keep:min1=10          \
  role:master:keep:hour1=6           \
  role:middle:keep:min1=30          \
  role:middle:keep:hour1=12          \
  role:sink:keep:min1=90            \
  role:sink:keep:hour1=24            \
  smtp:host=smtp.example.com        \
  smtp:port=587                      \
  smtp:from=zep@example.com          \
  smtp:to=admin@example.com          \
  smtp:user=zep@example.com          \
  smtp:password=xxx
```

Properties are synced to downstream nodes automatically during the first replication.

### 2. Set up SSH

Full-mesh passwordless SSH between all nodes for the replication user:

```bash
ssh-keyscan node1.local node2.local node3.local >> ~/.ssh/known_hosts
# distribute each node's public key to every other node's authorized_keys
```

### 3. Initial replication

```bash
zep --fs tank/data --init
```

### 4. Add to cron (every node)

```bash
* * * * * /usr/local/bin/zep --cron
```

Master replicates (interval-gated) and rotates; non-masters rotate silently.

## Usage

```
zep --fs <dataset> [--label <name>] [--keep <n>] [flags]
zep                                              # auto-discover all master datasets
```

### Arguments

| Flag | Description |
| :--- | :--- |
| `--fs <dataset>` | ZFS filesystem to operate on. Omit to auto-discover all local datasets where this node is master. |
| `--label <name>` | Snapshot label to replicate (repeatable). Omit to use all labels with a `zep:role:*:keep:` policy. |
| `--keep <n>` | Fallback retention count if no role/node policy is set (default: 10). |

### Primary Flags

| Flag | Description |
| :--- | :--- |
| `--init` | Initial replication — full stream, creates destination datasets. |
| `--cron` | Cron mode: master replicates (interval-gated) + rotates; non-master rotates only, exits 0. |
| `--now` | Force immediate snapshot creation for all labels, bypassing interval checks. |
| `--dry-run` | Simulate the entire chain without creating snapshots or transferring data. |
| `--status` | Chain health dashboard with pool capacity, IO stats, and snapshot freshness. |
| `--config` | Configuration mode — see below. |
| `--promote` | Promote this node to master. Requires `--auto`, `--snap <name>`, or `--align-chain-data`. |
| `--suspend` / `--resume` | Pause or resume master replication chain-wide. |
| `--alias <name>` | Override local node alias (when hostname detection fails). |
| `--stats` | Internal wire protocol for `--status`. |
| `-y` | Assume yes for destructive operations. |

### Advanced Flags

| Flag | Description |
| :--- | :--- |
| `--rotate` | Run retention purge for all labels locally (standalone, no replication). |
| `--target <node>` | Point-to-point transfer to a specific node (bypasses chain). |
| `--donor` | Run as a donor peer for downstream healing (internal). |
| `--apply-props <b>` | Apply encoded properties and exit (internal). |
| `--divergence-report <snap>` | Check remote dataset for split-brain (internal). |

### Configuration Mode (`--config`)

```bash
# View all zep:* properties
zep --fs tank/data --config

# Set properties (shorthands expand automatically)
zep --fs tank/data --config chain=node1,node2,node3 smtp:host=mail.example.com

# Clear a property
zep --fs tank/data --config --clear smtp:port

# Export / import
zep --fs tank/data --config --export /tmp/zep.conf
zep --fs tank/data --config --import /tmp/zep.conf
```

## Configuration Properties

All properties are ZFS user properties on the dataset prefixed with `zep:`.

### Chain & Identity

| Property | Description | Example |
| :--- | :--- | :--- |
| `zep:chain` | Comma-separated chain order. | `node1,node2,node3` |
| `zep:node:<alias>:fs` | Dataset path on that node. | `tank/data` |
| `zep:node:<alias>:fqdn` | FQDN or IP for SSH. | `10.0.0.5` |
| `zep:node:<alias>:user` | SSH user (per-node override). | `repluser` |
| `zep:user` | Global fallback SSH user. | `root` |
| `zep:snap_prefix` | Snapshot name prefix (default: `zep_`). | `zep_` |

### Retention

| Property | Example |
| :--- | :--- |
| `zep:role:master:keep:<label>` | `zep:role:master:keep:min1=10` |
| `zep:role:middle:keep:<label>` | `zep:role:middle:keep:hour1=12` |
| `zep:role:sink:keep:<label>` | `zep:role:sink:keep:min1=90` |
| `zep:node:<alias>:keep:<label>` | Per-node override. |

Labels encode their interval in the name: `min5`=5m, `hour2`=2h, `day1`=1d.

### Transfer

| Property | Default | Description |
| :--- | :--- | :--- |
| `zep:zfs:send_opt` | *(none)* | Extra `zfs send` flags (e.g. `-w`, `-L`). |
| `zep:zfs:recv_opt` | *(none)* | Extra `zfs recv` flags. `-F` is added automatically when force mode is active. |
| `zep:throttle` | *(none)* | Bandwidth limit for `mbuffer` (e.g. `100M`). |
| `zep:mbuffer_size` | `64M` | `mbuffer` in-memory buffer. |
| `zep:ssh:timeout` | `10` | SSH connect timeout (seconds). |
| `zep:proc:timeout` | `3600` | Total transfer timeout (seconds). |

### Policy

| Property | Default | Description |
| :--- | :--- | :--- |
| `zep:policy` | `fail` | `fail` (abort on error) or `resilience` (skip failed nodes). |
| `zep:suspend` | `false` | Set to `true` to pause master replication chain-wide. |

### Alerting

| Property | Default | Description |
| :--- | :--- | :--- |
| `zep:smtp_host` | — | SMTP server hostname. |
| `zep:smtp_port` | `465` | SMTP port. |
| `zep:smtp_protocol` | `smtps` | `smtp` or `smtps`. |
| `zep:smtp_user` | — | SMTP auth user. |
| `zep:smtp_password` | — | SMTP auth password. |
| `zep:smtp_from` | — | Envelope sender. |
| `zep:smtp_to` | — | Alert recipient. |
| `zep:alert:critical:threshold` | `0s` | Rate-limit for critical alerts. |
| `zep:alert:warn:threshold` | `1h` | Rate-limit for warnings. |
| `zep:alert:info:threshold` | `24h` | Rate-limit for info alerts. |
| `zep:alert:heartbeat:<label>` | — | Max age before triggering a heartbeat-lost alert (e.g. `2h`). |

### Housekeeping (managed by zep)

| Property | Purpose |
| :--- | :--- |
| `zep:shipped` | Set to `true` on snapshots that have reached the chain sink. Rotation purges shipped snapshots first. |
| `zep:error:split-brain` | Set to `true` on datasets where local writes were detected during receive. Cleared automatically when resolved. |
| `zep:node:<alias>:last_snapshot` | GUID of the most recent snapshot shipped to that node. Preserved during rotation so temporarily-offline nodes can resume without requiring a donor. |

### Temp Files

| Path | Purpose |
| :--- | :--- |
| `/tmp/<prefix>_<alias>-<ds>-<label>.lock` | Per-transfer lock file (PID-stamped, self-healing on stale PIDs). |
| `/tmp/<prefix>_<alias>-<ds>-<label>.lock.cnt` | Cumulative byte count written by `zpipe` during transfer. |
| `/tmp/<prefix>_<alias>-<ds>-replication.err` | Captured stderr from the `zfs send \| zfs recv` pipeline. |
| `/tmp/<prefix>_<alias>-<ds>-replication.hint` | Split-brain recovery hints displayed to the operator. |
| `/tmp/<prefix>_<alias>-<ds>-repl-alerts/` | Per-dataset rate-limit state for SMTP alerts. |

## Hardening — Minimal ZFS Permissions

Running replication as a non-root user requires delegated ZFS permissions. The
following two-tier delegation gives `zep` exactly what it needs and nothing more:

**Pool-level** (survives dataset destruction, tied to the pool):

```bash
zfs allow repluser create,mount,receive,destroy,send,snapshot,hold,release,userprop,diff tank
```

| Permission | Why needed |
| :--- | :--- |
| `create` | `zfs recv` creates target datasets on first receive. |
| `mount` | Received datasets must be mounted after create. |
| `receive` | Core receive side of `zfs send \| zfs recv`. |
| `destroy` | Rotation purges old snapshots; init tears down stale datasets. |
| `send` | Source side of send/recv pipeline. |
| `snapshot` | `zep` creates timestamped snapshots on the master. |
| `hold` | Holds prevent rotation from deleting in-flight snapshots. |
| `release` | Releases hold after transfer completes. |
| `userprop` | Read/write `zep:*` properties. |
| `diff` | Donor discovery and divergence reports use `zfs diff`. |

**Dataset-level** (must be re-delegated after `zfs destroy` / `zfs recv`):

```bash
zfs allow repluser create,destroy,send,receive,snapshot,hold,release,userprop tank/data
```

This mirrors the pool-level set but excludes `mount` (handled at pool level).
The replication user also needs **no** `sudo` access — all operations are
performed through the delegated ZFS permissions.

### SSH Hardening

```bash
# In ~/.ssh/authorized_keys on each node:
command="/usr/local/bin/zep",no-port-forwarding,no-X11-forwarding,no-agent-forwarding,no-pty ssh-rsa AAAA...
```

This restricts the replication user's key to only running `zep` — no shell, no
port forwarding, no PTY. Combine with `zep:user` to use a non-root account across
the chain:

```bash
zep --fs tank/data --config user=repluser
```

## Exit Codes

| Code | Meaning |
| :--- | :--- |
| `0` | Success — all nodes replicated. |
| `1` | Error — non-recoverable failure (unreachable master, invalid args, policy=fail abort). |
| `2` | Split-brain — data divergence detected; replication halted. |
| `3` | Partial success — resilience mode skipped one or more nodes. |

`--status` exit codes: `0` = all healthy, `1` = warnings, `2` = critical.

## Modular Structure

| File | Purpose |
| :--- | :--- |
| `zeplicator` | Orchestrator — arg parsing, mode dispatch, promotion, cascade logic. |
| `zep-common.lib.sh` | Property cache, logging, locks, node resolution, time parsing. |
| `zep-transfer.lib.sh` | Replication engine — `zfs send`/`recv` pipelines, donor discovery, split-brain. |
| `zep-retention.lib.sh` | Snapshot rotation — shipped-aware purge, retention resolution. |
| `zep-alerts.lib.sh` | SMTP alerting with per-dataset rate-limiting. |
| `zep-status.lib.sh` | `--status` dashboard — chain health, pool stats, snapshot freshness. |
| `zep-stats.lib.sh` | `--stats` wire protocol for remote status calls. |
| `zpipe.c` | C pipe monitor for byte-progress tracking in send/recv pipelines. |
| `Makefile` | Assembles libraries into `build/zep` and compiles `zpipe`. |

## Development

```bash
make                                 # build/zep + build/zpipe
bash tests/zep_replication_tests.sh  # run full test suite
bash tests/zep_replication_tests.sh 1   # run test 1 only
bash tests/zep_replication_tests.sh --list  # list all tests
```

### Test Environment

Tests simulate a 3-node ZFS replication chain on a single machine using:

| Component | Purpose |
| :--- | :--- |
| `tests/init.sh` | One-shot setup: creates a tmpfs ramdisk with sparse pool images, per-node ZFS datasets, `zep-user-{1..3}` accounts with delegated permissions, full-mesh SSH keys, and imports `test.conf` as the master config. |
| `tests/done.sh` | Full teardown: destroys pools, unmounts ramdisk, removes users, cleans `/etc/hosts` and crontab. |
| `tests/zep_replication_tests.sh` | 20-test suite exercising init, incremental, divergence, split-brain, resume, resilience, promotion, foreign dataset, missing perms/pool, status, rotate, and donor recovery. Supports `--test` and `--skip` filters. |
| `tests/tzepcon` | Launches a 4-pane tmux session: test output, SMTP debug (`alertcon`), live `zep --status`, and interactive simulator shell. |
| `build/alertcon` | Python debug SMTP server listening on `127.0.0.1:1025`. Captures all alert emails and serves them via `--log`, `--count`, and `--get` subcommands. |
| `tests/test.conf` | Central config: pool size, chain order, SMTP settings, alert thresholds. Sourced by `init.sh` and imported into the master dataset via `zep --config --import`. |

The simulated cluster uses mock DNS entries in `/etc/hosts` (`zep-node-{1..3}.local → 127.0.0.1`) and SSH connectivity through local zep-user accounts with delegated ZFS permissions (`zfs allow`).

See `tests/README.txt` for the interactive simulator cheatsheet.

## Credits

Core transfer logic adapted from `zfsbud.sh` by [Pawel Ginalski (gbyte.dev)](https://gbyte.dev).
