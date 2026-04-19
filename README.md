# Zeplicator: ZFS Replication Manager

A robust, cascading ZFS replication script designed for multi-node chains. It handles snapshot creation, incremental transfers with resume support, and graduated retention across the entire chain.

## Features

- **Cascading Replication**: Automatically triggers replication on the next hop in the chain once the local transfer is verified.
- **End-to-End Verification**: Confirms the arrival of the specific snapshot at the final sink before marking local snapshots as "shipped".
- **Automatic Configuration Sync**: All `repl:*` properties (retention, SMTP, chain order) are automatically propagated from the master to all downstream nodes during replication.
- **Safe Master Promotion**: Promote any node to Master using the `--promote` flag. Support for automatic chain-wide consistency checks and rollbacks.
- **Pause & Resume**: Use `--suspend` and `--resume` to globally pause Master operations.
- **Cron Safety**: The script can be configured in cron on **all nodes**. Only the current Master will initiate replication.
- **Robust Transfers**: Uses `zfsbud` logic with `mbuffer` and `zstd` compression for reliable and fast ZFS send/receive.
- **Graduated Retention**: Different retention policies (keep counts) for each node in the chain.
- **Skip-Hop Resiliency**: If a downstream node is offline, the script automatically skips it and attempts to replicate to the next node in the chain.
- **SMTP Alerts**: Sends email notifications for critical failures, initial sync success, role changes, and suspend/resume actions.
- **Modular Architecture**: Clean separation of concerns with specialized libraries for common utilities, alerts, retention, and transfers.

## Dependencies

The following packages must be installed on all nodes:

- `zfsutils-linux`
- `zfs-auto-snapshot`
- `openssh-server` / `openssh-client`
- `mbuffer`
- `zstd`
- `curl` (for SMTP alerts)

## Installation & Setup

1. **Clone & Build**:
   ```bash
   git clone https://github.com/arg7/zfs-replication.git
   cd zfs-replication
   ./build.sh  # Generates zeplicator-standalone.sh
   ln -s $(pwd)/zeplicator-standalone.sh /usr/local/bin/zeplicator
   ```

2. **SSH Connectivity**:
   Ensure **Full Mesh** SSH connectivity between all nodes. Use `ssh-keyscan` and distribute public keys to all `authorized_keys`.

3. **ZFS Properties**:
   Configure the Master node's dataset. All properties prefixed with `repl:` are automatically synced across the chain.

## Configuration Properties (`repl:*`)

| Property | Description | Example |
| :--- | :--- | :--- |
| `repl:chain` | **(Required)** Comma-separated list of host aliases. | `node1,node2,node3` |
| `repl:node:<alias>:fs` | Physical dataset path for a specific host alias. | `repl:node:node1:fs=tank/data` |
| `repl:node:<alias>:fqdn` | Real address/FQDN for a host alias. | `repl:node:node1:fqdn=10.0.0.5` |
| `repl:node:<alias>:user` | SSH user for a host alias. | `repl:node:node1:user=repluser` |
| `repl:node:<alias>:keep:<label>` | Host-specific retention. | `repl:node:node1:keep:min1=30` |
| `repl:role:<role>:keep:<label>` | Role-based retention (`master`, `middle`, `sink`). | `repl:role:sink:keep:min1=90` |
| `repl:user` | **(Global)** Fallback SSH user. | `root` |

## Usage

### Basic Replication
```bash
zeplicator pool/mydata min1 10
```

### Initial Replication
For the first run (no common snapshots downstream). Sends a success email upon completion.
```bash
zeplicator pool/mydata min1 10 --initial
```

### Master Promotion & Recovery
1. **Auto-Discovery (Recommended)**:
   ```bash
   zeplicator pool/mydata min1 10 --promote --auto [-y]
   ```
2. **Brutal Startover (Dangerous)**:
   ```bash
   zeplicator pool/mydata min1 10 --promote --destroy-chain
   ```

### Pause & Resume
```bash
zeplicator pool/mydata min1 10 --suspend
zeplicator pool/mydata min1 10 --resume
```

## Modular Structure
The project is split into several libraries for easier testing:
- `zfs-common.lib.sh`: Core utilities and property resolution.
- `zfs-alerts.lib.sh`: SMTP notification logic.
- `zfs-retention.lib.sh`: Snapshot rotation logic.
- `zfs-transfer.lib.sh`: The core replication engine.
- `zeplicator`: The main orchestrator script.

Use `./build.sh` to compile these into a single `zeplicator-standalone.sh` for distribution.

## Credits
This script incorporates core logic from `zfsbud.sh` by [Pawel Ginalski (gbyte.dev)](https://gbyte.dev).
