# Zeplicator Architecture

## Overview

Zeplicator is a modular ZFS replication manager designed for peer-to-peer donor discovery and split-brain safety. It manages snapshot-based replication across a chain of nodes, with per-dataset configuration stored directly on ZFS datasets as user properties.

The project uses a **Compiled Standalone** model: source files are assembled by `make` into a single self-contained `build/zep` script plus a compiled `build/zpipe` C binary.

### File Layout

```
src/
  zeplicator              # Main orchestrator (entry point)
  zfs-common.lib.sh       # Property cache, logging, locks, node resolution, base64 codec
  zfs-status.lib.sh       # Dashboard (--status): chain-wide health reporting
  zfs-stats.lib.sh        # Raw stat gathering (--stats): internal wire protocol called via SSH
  zfs-alerts.lib.sh       # SMTP alerting with per-dataset rate limiting
  zfs-retention.lib.sh    # Snapshot rotation with shipped-aware purge logic
  zfs-transfer.lib.sh     # Replication engine: zfs send/recv pipeline with split-brain detection
  zpipe.c                 # C pipe monitor for transfer byte-progress tracking
Makefile                  # Assembly: strips shebangs/source lines, concatenates into build/zep
build/                    # Output directory
  zep                    # Assembled standalone executable
  zpipe                  # Compiled C binary
```

### Assembly Order (function dependency)

```
zfs-common.lib.sh     →  colors, cache, logging, locks, defaults
zfs-stats.lib.sh      →  cmd_stats (uses common)
zfs-status.lib.sh     →  cmd_status (uses stats + common)
zfs-alerts.lib.sh     →  send_smtp_alert (uses common)
zfs-retention.lib.sh  →  resolve_retention, purge_shipped_snapshots (uses common)
zfs-transfer.lib.sh   →  zfsbud_core, send_snapshot (uses common + alerts)
zeplicator            →  orchestrator (uses all libs)
```

The Makefile strips the shebang from every file and removes `source` lines from the orchestrator before concatenating. Library boundaries are marked with `# --- BEGIN/END xxx ---` comments.

---

## Execution Model

`zeplicator` processes arguments in a strict order. Early-exit modes run and exit before the main replication path.

### Mode Dispatch Order

```
1. --divergence-report    Split-brain forensic analysis
2. Property cache seeding (--sync-props from upstream, or cache_zfs_props)
3. Local filesystem resolution (my_hostname, local_ds, temp paths)
4. --stats                Raw stat gathering (internal wire protocol, called via SSH)
5. --status               Chain-wide health dashboard
6. --rotate               Local snapshot retention/rotation
7. --config               Configuration management (list/clear/export/import/set/--all)
8. --suspend / --resume   Chain-wide suspend/resume
9. --apply-props          Internal: apply encoded properties and exit
10. --promote             Reorder chain: become new master
11. Main replication path  zfs send/recv cascade
```

### Main Replication Flow

```
┌─────────────────────────────────────────────────┐
│  MASTER NODE                                     │
│  1. Resolve identity (chain position, alias)     │
│  2. Validate label has retention policy          │
│  3. Check zep:suspend isn't true                 │
│  4. Create timestamped snapshot @zep_<label>-<ts>│
│  5. Acquire lock (stale PID self-healing)        │
│  6. Clear stale split-brain flag if present      │
│  7. For each downstream node:                    │
│     ├─ zfsbud_core() → build pipeline            │
│     ├─ On success: cascade to next via SSH       │
│     └─ On split-brain (exit 2): handle policy    │
│  8. Mark snapshot zep:shipped=true               │
│  9. Cleanup temp files, release lock             │
└─────────────────────────────────────────────────┘
```

---

## Property System

All configuration lives as `zep:*` ZFS user properties on the dataset. Properties are cached in-memory once at startup.

### Cache Architecture (`zfs-common.lib.sh:22-92`)

```
ZEP_PROP_CACHE               Associative array: key="dataset:property" → value
ZEP_PROP_DEFAULTS            Pre-seeded fallback values for commonly-unset props
```

**`cache_zfs_props(root_ds)`** — Populates cache by iterating `zfs list -r` over root and all children, calling `zfs get all` once per dataset. Batch-fetches per-node properties (`user`/`fqdn`/`fs`) in a single call for the root dataset.

**`get_zfs_prop(prop, ds)`** — Cache lookup only. Returns `"-"` on miss (never calls `zfs get`).

**`seed_cache_from_encoded(raw_ds, encoded, local_ds)`** — Populates cache from a `--sync-props` blob (avoids redundant `zfs get` on remote nodes). Handles both multi-dataset (`\x1f` delimited) and legacy single-dataset (`\x1e` delimited) formats. When `local_ds != raw_ds`, translates child dataset paths (e.g., `pool1/data/child` → `pool2/data/child`).

### Property Encoding (`zfs-common.lib.sh:292-326`)

**`get_repl_props_encoded(root_ds)`** — Serializes the in-memory cache for all datasets under root into a base64-encoded blob using `\x1f` (Unit Separator) for dataset delimiters and `\x1e` (Record Separator) for property delimiters.

**`apply_repl_props(ds, encoded)`** — Decodes the blob, finds the section matching `ds`, and applies properties via `zfs set`. When multi-dataset and exact match fails (remote `local_ds` differs), falls back to the first root section.

### Key ZFS Properties

| Property | Purpose |
|---|---|
| `zep:chain` | Comma-separated node alias list |
| `zep:node:<alias>:fs` | Dataset path on that node |
| `zep:node:<alias>:fqdn` | FQDN for SSH |
| `zep:node:<alias>:user` | SSH user |
| `zep:snap_prefix` | Snapshot name prefix (default: `zep_`) |
| `zep:policy` | `fail` or `resilience` |
| `zep:suspend` | `true` to pause chain-wide |
| `zep:role:<role>:keep:<label>` | Role-based retention count |
| `zep:alert:heartbeat:<label>` | Stale-snapshot detection threshold |
| `zep:error:split-brain` | Set to `true` when split-brain detected |
| `zep:throttle` | Bandwidth limit |
| `zep:mbuffer_size` | mbuffer buffer size |
| `zep:zfs:send_opt` | Extra zfs send flags |
| `zep:zfs:recv_opt` | Extra zfs recv flags (default: none; `-F` added automatically when force mode is active) |

---

## Replication Pipeline

The core pipeline is built by `send_snapshot()` in `zfs-transfer.lib.sh:240`:

```
zfs send [flags] 2>>err_log \
  | zpipe <lock_path> 1 [timeout] \
  | mbuffer -q -r <throttle> -m <mbuffer_size> 2>>err_log \
  | zstd 2>>err_log \
  | ssh -o ConnectTimeout=<t> "zstd -d | zfs recv [flags] <remote_ds>" 2>>err_log
```

### Send/Recv Flags

User-configurable via `zep:zfs:send_opt` (default empty) and `zep:zfs:recv_opt` (default empty; `-F` injected automatically when needed). Injected directly — no boolean-to-flag translation.

Three send modes based on context:
- **Resume**: `zfs send -v -t <token>` + `zfs recv -u`
- **Initial**: `zfs send -v -R <latest>` + `zfs recv -u -o canmount=noauto`
- **Incremental**: `zfs send -v -R -i <common> <latest>` + `zfs recv -u -o canmount=noauto`

### Split-Brain Detection

When the pipeline produces `"cannot receive incremental stream"` + `"has been modified"`, the engine:
1. Runs `zep --divergence-report` on the remote via SSH
2. Prints recovery hints to `REPL_HINT_FILE`
3. Sets `zep:error:split-brain=true` on the remote dataset
4. Sends a critical SMTP alert
5. Returns exit code 2

### Exit Codes

| Code | Meaning |
|---|---|
| 0 | Success |
| 1 | General failure |
| 2 | Split-brain detected |
| 3 | Partial success (resilience policy) |

### Policy Handling

- **`fail`**: Aborts the entire job on any error
- **`resilience`**: Skips failed nodes, continues chain, returns exit code 3

---

## Status Dashboard (`--status`)

`cmd_status()` in `zfs-status.lib.sh:29` iterates every node in the chain, pings each, then calls `get_node_state()` which SSHs to remote nodes and runs `zep --stats`.

With the `--sync-props` optimization, the calling node encodes its property cache into a base64 blob and passes it to remote nodes. The remote node seeds its cache from the blob, avoiding redundant `zfs get` calls.

### Output Format

- Per-node header with ping time and aggregated status
- Per-pool health, capacity, and IO stats
- Per-dataset snapshot age vs heartbeat threshold
- Color coding: green → yellow at 5× heartbeat, yellow → red at 10× heartbeat
- Split-brain forces red
- Retention percentage display
- Active transfer progress badge
- Global exit code: 0=all green, 1=yellow exists, 2=red exists

### Stats Wire Protocol (`--stats`)

Four record types, one per line:

```
ZPOOL:<pool> <health> <capacity%>
IOSTAT:<pool> <ops_r> <ops_w> <bw_r> <bw_w>
FILESYSTEM|<ds>|<label>|<snap>|<age_min>|<configured>|<heartbeat>|<has_split_brain>|<snap_count>|<keep_val>
TRANSFER|<ds_safe>|<estimated_size>|<actual_bytes>
```

---

## Snapshot Lifecycle

### Naming Convention

```
<prefix><label>-<YYYY-MM-DD-HHMM>   (or with seconds for collision avoidance)
Example: zep_hourly-2025-01-15-1430
```

### Creation

Only the master creates snapshots. The snapshot name includes a UTC timestamp to guarantee uniqueness.

### Retention (`zfs-retention.lib.sh`)

`purge_shipped_snapshots(ds, label, keep_count)`:
- Collects snapshots matching the label + prefix, sorted newest-first
- **Shipped-aware**: If any snapshot within the keep window is marked `shipped=true`, older unshipped snaps are safe to delete (covered by a newer shipped one). Otherwise, unshipped older snaps are preserved.
- Resolving retention: checks `zep:role:<role>:keep:<label>` and `zep:node:<ME>:keep:<label>`

### Heartbeat Monitoring

Non-master nodes check `zep:alert:heartbeat:<label>`. If the latest snapshot for that label exceeds the threshold, a critical alert fires.

---

## IO Monitoring (`zpipe.c`)

A minimal C utility that sits between `zfs send` and `mbuffer`:

```
stdin → [read 1MB chunks] → [count bytes] → stdout
```

- Writes cumulative byte count to `<lock_base>.cnt` every N seconds
- On `SIGTERM`/`SIGINT`, writes final count and exits cleanly
- Optional timeout: exits 143 if elapsed exceeds `timeout_sec`
- On EOF (pipe end), writes final count and exits 0

---

## Lock Management

Lock file: `/tmp/<prefix>_<alias>-<ds-safe>-<label>.lock`

**Self-healing**: If the PID in the lockfile is no longer alive (`kill -0` fails), the lock is automatically removed.

**Stuck job detection**: If a lock exceeds `zep:proc:timeout` (default 60s), checks `.cnt` file progress. If progress is flat over 2s, alerts and aborts. If still progressing, touches lock to reset age.


---

## Alerting (`zfs-alerts.lib.sh`)

SMTP delivery via `curl` with `smtps://` protocol. Configuration from `zep:smtp_*` properties.

Rate limiting per dataset, per md5sum of `level:message`:
- Default thresholds: critical=0s, warn=3600s, info=86400s
- Customizable via `zep:alert:<level>:threshold`

Suppressed alerts accumulate a count; a summary note is appended when the alert is finally sent.

---

## Temp File Conventions

```
/tmp/<prefix>_<alias>-<ds-safe>-<label>.lock        # Lock file
/tmp/<prefix>_<alias>-<ds-safe>-<label>.lock.cnt    # Transfer byte count
/tmp/<prefix>_<alias>-<ds-safe>-replication.err      # Pipeline stderr capture
/tmp/<prefix>_<alias>-<ds-safe>-replication.hint     # Split-brain recovery hints
/tmp/<prefix>_<alias>-<ds-safe>-repl-alerts/         # Rate-limit state directory
/tmp/<prefix><cmd>-<alias>-<uid>.log                 # Audit/execution log
```

---

## Indentation Convention (`CHAIN_PREFIX`)

Cascaded output uses a tree-drawing prefix:
- Master: empty
- First cascade hop: `"  | "`
- Each subsequent hop: prepends another `"  | "`

---

## Audit Wrappers

`zfs`, `zpool`, and `ssh` are shell function wrappers in `zfs-common.lib.sh:6-19` that log `AUDIT: <cmd> <args>` before executing the real command via `command zfs "$@"`.

---

## Key Code Locations

| Component | File | Lines |
|---|---|---|
| Orchestrator entry & arg parsing | `zeplicator` | 1–170 |
| Mode dispatch order | `zeplicator` | 250–330 |
| Main replication loop | `zeplicator` | 883–1367 |
| Cascade downstream via SSH | `zeplicator` | 1280–1330 |
| Promotion flow | `zeplicator` | 608–872 |
| Configuration (`--config`) | `zeplicator` | 447–561 |
| `sync_chain_props` (config push) | `zeplicator` | 380–440 |
| Property cache seeding | `zeplicator` | 264–297 |
| Property cache definitions | `zfs-common.lib.sh` | 22–92 |
| Node resolution (`get_local_alias`, etc.) | `zfs-common.lib.sh` | 95–286 |
| `get_repl_props_encoded` | `zfs-common.lib.sh` | 292–326 |
| `apply_repl_props` | `zfs-common.lib.sh` | 328–388 |
| `seed_cache_from_encoded` | `zfs-common.lib.sh` | 390–440 |
| Lock management (`check_stuck_job`) | `zfs-common.lib.sh` | 480–560 |
| `cmd_status` (dashboard) | `zfs-status.lib.sh` | 29–318 |
| `get_node_state` (SSH stats call) | `zfs-status.lib.sh` | 4–27 |
| `cmd_stats` (wire protocol) | `zfs-stats.lib.sh` | 4–121 |
| `send_smtp_alert` | `zfs-alerts.lib.sh` | 4–121 |
| `resolve_retention` | `zfs-retention.lib.sh` | 6–31 |
| `purge_shipped_snapshots` | `zfs-retention.lib.sh` | 33–100 |
| `zfsbud_core` (replication engine) | `zfs-transfer.lib.sh` | 115–452 |
| `send_snapshot` (pipeline builder) | `zfs-transfer.lib.sh` | 240–379 |
| `find_best_donor` (donor discovery) | `zfs-transfer.lib.sh` | 35–86 |
| `divergence_report` (split-brain analysis) | `zfs-transfer.lib.sh` | 88–113 |
| `zpipe.c` (pipe monitor) | `zpipe.c` | 1–82 |
| Assembly / build | `Makefile` | 1–50 |
