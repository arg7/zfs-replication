# Zeplicator: Another ZFS Replication Manager

A robust, cascading ZFS replication script designed for multi-node chains. It handles snapshot creation, incremental transfers with resume support, and graduated retention across the entire chain.

## Features

- **Split-Brain Protection**: Automatically performs a `zfs diff` between the common base and the local dataset on the sink. Replication **aborts** with a critical alert if any local data changes are detected, preventing silent data loss.
- **Intelligent Donor Discovery**: Downstream nodes can automatically discover and "pull" from any other node in the chain (not just their immediate parent) to find the best common snapshot, ensuring resilience even if multiple nodes are out of sync.
- **GUID-based Consistency**: Uses internal ZFS GUIDs instead of snapshot names for all intersection checks, making the system immune to snapshot renaming or formatting differences.
- **Visual Progress & Icons**: Rich CLI output with status icons (✅, ❌, 🔗) and a final success/partial-success summary for complex operations like promotion.
- **Cascading Replication**: Automatically triggers replication on the next hop in the chain once the local transfer is verified.
- **End-to-End Verification**: Confirms the arrival of the specific snapshot at the final sink before marking local snapshots as "shipped".
- **Automatic Configuration Sync**: All `zep:*` properties (retention, SMTP, chain order) are automatically propagated from the master to all downstream nodes during replication.
- **Safe Master Promotion**: Promote any node to Master using the `--promote` flag. Support for automatic chain-wide consistency checks and rollbacks.
- **Pause & Resume**: Use `--suspend` and `--resume` to globally pause Master operations.
- **Cron Safety**: The script can be configured in cron on **all nodes**. Only the current Master will initiate replication.
- **Robust Transfers**: Uses `zfsbud` logic with `mbuffer` and `zstd` compression for reliable and fast ZFS send/receive.
- **Graduated Retention**: Different retention policies (keep counts) for each node in the chain.
- **Skip-Hop Resiliency**: If a downstream node is offline, the script automatically skips it and attempts to replicate to the next node in the chain.
- **SMTP Alerts**: Sends email notifications for critical failures, initial sync success, role changes, and suspend/resume actions.
- **Dry-Run Simulation**: Full-chain "what-if" mode using `--dry-run`. It simulates snapshot creation, transfers, and rotation across all nodes using virtual snapshot propagation.
- **Modular Architecture**: Clean separation of concerns with specialized libraries for common utilities, alerts, retention, and transfers.

## Dependencies

The following packages must be installed on all nodes:

- `zfsutils-linux`
- `openssh-server` / `openssh-client`
- `mbuffer`
- `zstd`
- `curl` (for SMTP alerts)

## Installation & Setup

1. **Clone & Build**:
   ```bash
   git clone https://github.com/arg7/zfs-replication.git
   cd zfs-replication
   ./build.sh  # Generates zep-standalone.sh
   ln -s $(pwd)/zep-standalone.sh /usr/local/bin/zep
   ```

2. **SSH Connectivity**:
   Ensure **Full Mesh** SSH connectivity between all nodes. Use `ssh-keyscan` and distribute public keys to all `authorized_keys`.

3. **ZFS Properties**:
   Configure the Master node's dataset. All properties prefixed with `zep:` are automatically synced across the chain.

## Configuration Properties (`zep:*`)

| Property | Description | Example |
| :--- | :--- | :--- |
| `zep:chain` | **(Required)** Comma-separated list of host aliases. | `node1,node2,node3` |
| `zep:node:<alias>:fs` | Physical dataset path for a specific host alias. | `zep:node:node1:fs=tank/data` |
| `zep:node:<alias>:fqdn` | Real address/FQDN for a host alias. | `zep:node:node1:fqdn=10.0.0.5` |
| `zep:node:<alias>:user` | SSH user for a host alias. | `zep:node:node1:user=repluser` |
| `zep:node:<alias>:keep:<label>` | Host-specific retention. | `zep:node:node1:keep:min1=30` |
| `zep:role:<role>:keep:<label>` | Role-based retention (`master`, `middle`, `sink`). | `zep:role:sink:keep:min1=90` |
| `zep:user` | **(Global)** Fallback SSH user. | `root` |
| `zep:zfs:raw` | Whether to use `zfs send -w` (raw, includes properties). Default: `false`. | `true` |
| `zep:zfs:resume` | Whether to use `zfs recv -s` (resume support). Default: `false`. | `true` |
| `zep:zfs:force` | If `false`, omits `-F` from `zfs receive` (Safe Mode). Default: `true`. | `false` |
| `zep:throttle` | Bandwidth limit for `mbuffer` (e.g., `100M`, `1G`). Uses `-R`. | `50M` |
| `zep:mbuffer_size` | Size of `mbuffer` in-memory buffer. Default: `64M`. | `256M` |

### Configuration Management (`--config`)

Zep provides a built-in configuration engine to manage `zep:*` ZFS properties without needing to call `zfs set` manually. It supports shorthand prefixes for common settings.

#### Available Subcommands:

| Subcommand | Description |
| :--- | :--- |
| `--list` | (Default) Lists all `zep:` properties currently set on the dataset. |
| `key=value` | Sets a property. Supports shorthands like `smtp:host=...` or `node:n1:fqdn=...`. |
| `--clear <key>` | Inherits/removes a specific property from the dataset. |
| `--export <file>`| Saves all `zep:` properties to a plain-text file. |
| `--import <file>`| Loads and sets properties from a file. Supports comments and shorthands. |

#### Usage Examples:

**1. Viewing Configuration**
```bash
zep pool/mydata --config  # or --config --list
```

**2. Setting Properties (with Shorthands)**
Shorthands automatically expand to their full ZFS property names:
- `smtp:host=mail.com` $\rightarrow$ `zep:smtp_host=mail.com`
- `node:n1:fqdn=10.0.0.1` $\rightarrow$ `zep:node:n1:fqdn=10.0.0.1`
- `role:sink:keep:min1=90` $\rightarrow$ `zep:role:sink:keep:min1=90`

```bash
zep pool/mydata --config smtp:host=smtp.gmail.com smtp:port=587 node:node1:user=repl
```

**3. Clearing a Property**
```bash
zep pool/mydata --config --clear smtp:port
```

**4. Batch Import/Export**
```bash
# Export settings from one dataset
zep pool/old-data --config --export /tmp/repl.conf

# Import to another dataset
zep pool/new-data --config --import /tmp/repl.conf
```

### Usage

### Basic Replication
```bash
zep pool/mydata min1 10
```

### Explicit Identity Override
If auto-discovery fails (hostname doesn't match and IP isn't in DNS/config), you can force the node's alias:
```bash
zep pool/mydata min1 10 --alias node2
```

### Initial Replication
For the first run (no common snapshots downstream). Sends a success email upon completion.
```bash
zep pool/mydata --initial
```

### Master Promotion & Recovery
1. **Auto-Discovery (Recommended)**:
   ```bash
   zep pool/mydata --promote --auto [-y]
   ```
2. **Brutal Startover (Dangerous)**:
   ```bash
   zep pool/mydata --promote --destroy-chain
   ```

### Pause & Resume
```bash
zep pool/mydata --suspend
zep pool/mydata --resume
```

### Dry-Run & Simulation
Simulate the entire replication chain, including virtual snapshot creation and rotation previews, without making any changes:
```bash
zep pool/mydata min1 10 --dry-run
```

### Help & Flags
For a full list of all available flags and configuration options:
```bash
zep --help
```

## Modular Structure
The project is split into several libraries for easier testing:
- `zfs-common.lib.sh`: Core utilities and property resolution.
- `zfs-alerts.lib.sh`: SMTP notification logic.
- `zfs-retention.lib.sh`: Snapshot rotation logic.
- `zfs-transfer.lib.sh`: The core replication engine.
- `zeplicator`: The main orchestrator script.

Use `./build.sh` to compile these into a single `zep-standalone.sh` for distribution.

## Credits
This script incorporates core logic from `zfsbud.sh` by [Pawel Ginalski (gbyte.dev)](https://gbyte.dev).
