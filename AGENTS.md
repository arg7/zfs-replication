# Zeplicator Development Guide

## Project Architecture

Zeplicator is a modular ZFS replication manager designed for peer-to-peer donor
discovery and split-brain safety. It uses a "Compiled Standalone" model for
deployment.

### File Structure
- **`zep-common.lib.sh`**: Shared utilities — property cache, logging, lock management, time parsing.
- **`zep-alerts.lib.sh`**: SMTP alerting with per-dataset rate-limiting.
- **`zep-retention.lib.sh`**: Snapshot rotation — shipped-aware purge, retention resolution.
- **`zep-transfer.lib.sh`**: Core replication engine — `zfs send`/`recv` pipeline, donor discovery, split-brain detection.
- **`zep-status.lib.sh`**: `--status` dashboard — chain health, pool stats, snapshot freshness.
- **`zep-stats.lib.sh`**: `--stats` wire protocol — called remotely via SSH for status aggregation.
- **`zeplicator`**: Main orchestrator — arg parsing, mode dispatch, promotion, cascade logic.
- **`zpipe.c`**: C utility for byte-progress tracking in `zfs send | zfs recv` pipelines.
- **`Makefile`**: Assembly script — strips shebangs and `source` lines, concatenates libraries and orchestrator into `build/zep`; compiles `zpipe.c`.
- **`build/zep`**: The production-ready standalone script.
- **`build/zpipe`**: Compiled C binary for IO monitoring.
- **`build/alertcon`**: Debug SMTP server for development/testing.

### Assembly Order (function dependency)

```
zep-common.lib.sh     →  colors, cache, logging, locks, defaults
zep-stats.lib.sh      →  cmd_stats (uses common)
zep-status.lib.sh     →  cmd_status (uses stats + common)
zep-alerts.lib.sh     →  send_smtp_alert (uses common)
zep-retention.lib.sh  →  resolve_retention, purge_shipped_snapshots (uses common)
zep-transfer.lib.sh   →  send_snapshot, find_best_donor (uses common + alerts)
zeplicator            →  orchestrator (uses all libs)
```

## Critical Development Gotchas

### 1. The Assembly Trap
**Never** manually append library content into `zeplicator` or the standalone script.
- The `Makefile` handles assembly. It strips shebangs and `source` commands, then concatenates everything into `build/zep`.
- If you manually paste code into `zeplicator`, the standalone will end up with **multiple definitions** of the same functions, leading to silent failures where your changes are ignored.

### 2. Property Resolution (`get_zfs_prop`)
- `get_zfs_prop` returns the DEFAULTS value if a property is not set. 
- Send/receive flags live in two user-configurable string properties:
  ```bash
  local send_extra=$(get_zfs_prop "zep:zfs:send_opt" "$ds")
  local recv_extra=$(get_zfs_prop "zep:zfs:recv_opt" "$ds")
  # DEFAULTS: send_opt=""  recv_opt=""
  ```
  These are injected directly into the pipeline — no boolean-to-flag translation needed.
- `-F` is added to `recv_opt` automatically when force mode is active (divergence override).

### 3. IO Monitoring (`zpipe`)
- All `zfs send | zfs recv` pipelines MUST include `zpipe` for progress tracking.
- `zpipe` updates a `.cnt` file associated with the `.lock` file.
- Stale locks are automatically cleared if the PID in the lockfile is no longer found in the process table (`kill -0 $PID`).

### 4. Label Interval Parsing
- `parse_label_to_seconds()` in `zep-common.lib.sh` translates label names to seconds:
  `min5`→300, `hour2`→7200, `day1`→86400.
- Used by the interval gate to decide whether a snapshot is due.
- `--now` bypasses the check; `--init` also bypasses (always sends).

### 5. Testing & Deployment
- When testing changes, you MUST:
  1. Edit source files (`*.lib.sh`, `zeplicator`, `zpipe.c`).
  2. Run `make`.
  3. Deploy `build/zep` to `/usr/local/bin/zep` (and `zpipe` to `/usr/local/bin/zpipe`).
- Failure to run `make` means your changes only exist in source files and not in the actual executable.
- Test bench with Docker: `docker exec node1 /scripts/build/zep --fs tank/data --cron`.
- Local test bench: `./build/zep --fs zep-node-1/test-1 --alias node1`.

## Deployment Workflow
```bash
make
cp build/zep /usr/local/bin/zep
cp build/zpipe /usr/local/bin/zpipe

# Cron (every node)
* * * * * /usr/local/bin/zep --cron
```

## Test Infrastructure

### Overview

Tests simulate a multi-node ZFS replication cluster entirely on a single machine using:
- **tmpfs ramdisk** (`/tmp/zep-ramdisk`) — holds per-node sparse pool images
- **`/etc/hosts`** — mock DNS resolving `zep-node-$i.local` → `127.0.0.1`
- **System user accounts** (`zep-user-1` .. `zep-user-N`) — simulate per-node replication users with delegated ZFS permissions
- **Full-mesh SSH** — each zep-user's pubkey is distributed to every other zep-user and to root

Test scripts live in `tests/` and are run directly — they are **not** part of the `make` build.

### File Reference

| File | Purpose |
|---|---|
| `tests/test.conf` | Central config — pool size, chain order, SMTP settings, alert thresholds |
| `tests/init.sh` | One-shot setup: ramdisk, pools/datasets, zep-user accounts, SSH mesh, master config import |
| `tests/done.sh` | Full teardown: destroy pools, unmount ramdisk, remove users, clean `/etc/hosts` and crontab |
| `tests/zep_replication_tests.sh` | Main 20-test suite (see table below) |
| `tests/tzepcon` | Interactive 4-pane tmux session for live test observation |
| `tests/sim.sh` | Convenience wrappers in the tmux simulator pane: `start`, `stop`, `config`, `q` |
| `tests/README.txt` | User-facing cheatsheet displayed in the simulator pane |

### Simulated Cluster Architecture

All "nodes" are local ZFS pools named `zep-node-1` .. `zep-node-N`, each with one dataset `zep-node-$i/test-$i`:

| Node | Pool | Dataset | User | Role |
|---|---|---|---|---|
| node1 | `zep-node-1` | `test-1` | `zep-user-1` | Master (source of truth) |
| node2 | `zep-node-2` | `test-2` | `zep-user-2` | Middle/replica |
| node3 | `zep-node-3` | `test-3` | `zep-user-3` | Sink (last in chain) |

### Pool and Dataset Permissions

`init.sh` delegates minimal ZFS permissions per zep-user:

- **Pool-level** (`zfs allow` on the pool): `create,mount,receive,destroy,userprop,diff`
  - `create` + `mount` are required for `zfs recv` to create/receive datasets
  - These survive dataset destruction (tied to the pool, not the dataset)
- **Dataset-level** (`zfs allow` on the specific dataset): `create,destroy,send,receive,snapshot,hold,release,userprop`
  - These are LOST if the dataset is destroyed — must be re-delegated after recreate

### SSH Mesh Setup (init.sh)

1. Each zep-user gets a generated RSA keypair in their home `.ssh/`
2. The current user's SSH pubkey is copied to each zep-user's `authorized_keys`
3. Full mesh: every zep-user's pubkey is appended to every other zep-user's `authorized_keys`
4. All zep-users also get their pubkey added to root's `authorized_keys`
5. `known_hosts` is propagated from root to all zep-users
6. All `authorized_keys` are deduplicated with `sort -u`

**Gotcha**: If the test runner's SSH key isn't in the zep-user accounts, `zep --status` and `--init` pre-flight checks will fail with connection timeouts.

### Alert Testing

The test suite expects `alertcon` listening on port 1025 to capture SMTP alerts. `zep_replication_tests.sh` auto-launches it via `_ensure_alertcon()` if not already running — no manual step needed.

### The 20 Tests

Tests 1-10 are deterministic unit tests. Tests 11-13 exercise resilience and recovery. Tests 14-20 test promotion, status, rotate, donor recovery, foreign dataset, missing permissions/pools.

| # | Name | What it does | Expect |
|---|---|---|---|
| 1 | `INIT_CLEAN` | Destroys node2+3, runs multi-label `--init`, verifies GUID on all 3 nodes | exit 0 |
| 2 | `INCREMENTAL` | Writes data, runs incremental replication with explicit label | exit 0 |
| 3 | `DIVERGENCE` | GUID mismatch + data divergence tests on node3 | exit !0 |
| 4 | `DIVERGENCE_OVERRIDE` | Force override with `-F` recv flag | exit 0 |
| 5 | `RESUME` | `zpipe --cut` interrupts send at 1MB; retries resume up to 30x | exit 0 |
| 6 | `RESUME_FAILED` | `zpipe --cut` interrupts multi-snap send; destroys mid-transmit snaps; verifies token cleared | exit 0 |
| 7 | `RESILIENCE NODE2 OFFLINE` | `policy=resilience`, isolates node2, verifies node3 still receives | exit 3 |
| 8 | `RESILIENCE NODE2 RECOVERY` | Restores node2, full replication succeeds | exit 0 |
| 9 | `SPLIT-BRAIN RESILIENCE` | Split-brain with resilience policy — skipped, chain continues | exit 3 |
| 10 | `SPLIT-BRAIN ROLLBACK` | Resolves split-brain via rollback | exit 0 |
| 11 | `DIVERGENCE REPORT` | `--divergence-report` on clean and divergent states | exit 2/0 |
| 12 | `PROMOTE TO NODE3` | Promotes node3 to master, verifies chain reorder | exit 0 |
| 13 | `PROMOTE BACK TO NODE1` | Promotes node1 back, chain restored | exit 0 |
| 14 | `NON-MASTER SKIP` | Runs as node2 (non-master), expects "not Master" | exit !0 |
| 15 | `STATUS` | Runs `--status`, verifies all 3 node names appear | exit 0 |
| 16 | `ROTATE` | Creates manual snapshots, runs `--rotate`, verifies unshipped remain | count |
| 17 | `LOST COMMON / DONOR RECOVERY` | Node2 offline, aggressive rotation, verifies recovery without donor | exit 0 |
| 18 | `FOREIGN DATASET` | Creates alien snapshot on node3, expects `FOREIGN DATASET` | exit !0 |
| 19 | `MISSING PERMISSIONS` | Revokes pool perms on node3, expects "Missing pool permissions" | exit !0 |
| 20 | `MISSING POOL` | Exports node3's pool, expects "not found" | exit !0 |

### Test Flow Patterns

- Tests **mutate shared ZFS state** — they are NOT fully isolated.
- Tests must be run **sequentially in order** (1→2→…→20).
- Most tests start with `_pre_test_cleanup` which destroys node2+3 datasets.
- Each `run_zep()` call clears `/tmp/zep_*` (lock/counter files) first.

**Helper functions** in `zep_replication_tests.sh`:
- `run_zep()` — clears tmp, runs `zep` with `--now` auto-injected, returns stdout and exit code
- `assert_exit(name, expected, actual)` — 0, !0, or exact value
- `assert_out(name, output, pattern)` — grep for pattern in output
- `_assert_snap_on_node(node, guid)` — verify a specific GUID exists on a node
- `_latest_master_guid()` — GUID of latest `@zep_` snapshot on master
- `_ensure_alertcon()` — auto-launches `alertcon` daemon if not running
- `destroy_node3()` — `zfs destroy -r zep-node-3/test-3`
- `clean_tmp()` — `rm -rf /tmp/zep_*`
- `isolate_node(n)` / `restore_node(n)` — toggle FQDN to simulate offline/online

**Test filtering**: pass test IDs as positional args: `zep_replication_tests.sh 1 2 3` runs only those.

### Running Tests

```bash
# Full suite (must be root or have ZFS privileges)
bash tests/zep_replication_tests.sh

# Specific tests
bash tests/zep_replication_tests.sh 1 9 15

# Interactive tmux dashboard
bash tests/tzepcon 1 2 3

# Teardown
bash tests/done.sh
```

### Test Development Gotchas

1. **ZFS permissions are volatile on datasets**: When you `zfs destroy` and `zfs recv` recreates a dataset, dataset-level delegated permissions are lost. Pool-level permissions survive. Always re-delegate dataset perms after recreation.
2. **`/tmp/zep_*` cleanup**: Stale lock/counter files block replication. `run_zep()` calls `clean_tmp()` before every invocation.
3. **SSH key distribution**: If you change the user running tests, re-run `init.sh` to distribute the new user's SSH key.
4. **Ramdisk is tmpfs**: Data is in memory. On reboot or unmount, all pools are gone. Re-run `init.sh`.
5. **No `make` needed for test scripts**: `make` only rebuilds `build/zep`/`build/zpipe`. Test scripts run as-is. But `build/zep` must be up-to-date.
6. **Sequential dependency**: Tests build on each other's state. Running test 5 in isolation will fail.
7. **`zfs-auto-snap` interference**: The system `zfs-auto-snap` cron job creates pool-level snapshots with different GUIDs across nodes, triggering false-positive GUID_MISMATCH errors. Tests are designed to tolerate this, but be aware during debugging.

## Git Repository
- **Commit & Push**: Do not stage, commit, or push any changes unless specifically requested by the user. When asked to commit or prepare a commit, always start by gathering information using `git status` and `git diff HEAD`, then propose a draft commit message. Never push changes to a remote repository without an explicit instruction to do so.
