# ZFS Replication Manager

A robust, cascading ZFS replication script designed for multi-node chains. It handles snapshot creation, incremental transfers with resume support, and graduated retention across the entire chain.

## Features

- **Cascading Replication**: Automatically triggers replication on the next hop in the chain once the local transfer is verified.
- **End-to-End Verification**: Confirms the arrival of the specific snapshot at the final sink before marking local snapshots as "shipped".
- **Automatic Configuration Sync**: All `repl:*` properties (retention, SMTP, chain order) are automatically propagated from the master to all downstream nodes during replication.
- **Safe Master Promotion**: Promote any node to Master using the `--promote` flag. Support for automatic chain-wide consistency checks and rollbacks.
- **Pause & Resume**: Use `--suspend` and `--resume` to globaly pause Master operations.
- **Cron Safety**: The script can be configured in cron on **all nodes**. Only the current Master will initiate replication.
- **Robust Transfers**: Uses `zfsbud` logic with `mbuffer` and `zstd` compression for reliable and fast ZFS send/receive.
- **Graduated Retention**: Different retention policies (keep counts) for each node in the chain.
- **Skip-Hop Resiliency**: If a downstream node is offline, the script automatically skips it and attempts to replicate to the next node in the chain to ensure data flow continues.
- **Pre-flight Health Checks**: Performs quick connectivity tests before starting transfers to avoid long timeouts.
- **Transfer Timeouts**: Prevents replication from hanging indefinitely during ZFS send/receive operations.
- **SMTP Alerts**: Sends email notifications for critical failures, stuck jobs, role changes, and suspend/resume actions.

## Dependencies

The following packages must be installed on all nodes in the replication chain:

- `zfsutils-linux`
- `zfs-auto-snapshot`
- `openssh-server` / `openssh-client`
- `mbuffer`
- `zstd`
- `curl` (for SMTP alerts)

## Installation & Setup

1. **Clone & Link**:
   ```bash
   git clone https://github.com/arg7/zfs-replication.git
   ln -s $(pwd)/zfs-replication/zfs-replication.sh /usr/local/bin/zfs-replication.sh
   ```

2. **SSH Connectivity**:
   Ensure **Full Mesh** SSH connectivity between all nodes. Use `ssh-keyscan` and distribute public keys to all `authorized_keys`.

3. **ZFS Properties**:
   Configure the Master node's dataset. All properties prefixed with `repl:` are automatically synced across the chain.

## Configuration Properties (`repl:*`)

The script uses ZFS user properties for configuration. These should be set on the Master dataset and will propagate downstream.

| Property | Description | Example |
| :--- | :--- | :--- |
| `repl:chain` | **(Required)** Comma-separated list of hostnames in order. | `node1,node2,node3` |
| `repl:<hostname>` | Physical pool name for a specific host. | `repl:node1=tank` |
| `repl:<label>` | Comma-separated retention (keep counts) for each node. | `repl:min1=10,30,90` |
| `repl:user` | SSH user for replication (default: `root`). | `root` |
| `repl:timeout` | Seconds before a job is considered stuck (default: `3600`). | `7200` |
| `repl:suspend` | Globally pause replication if set to `true`. | `false` |
| `repl:smtp_host` | SMTP server for alerts. | `smtp.example.com` |
| `repl:smtp_port` | SMTP port (default: `465`). | `465` |
| `repl:smtp_to` | Destination email for alerts. | `admin@example.com` |

### Pool Name Resolution
To allow uniform commands across nodes, the script maps logical paths to physical pools:
1. It looks for `repl:<hostname>` on the dataset.
2. If not found, it checks for a pool named `pool`.
3. Fallback: `$(hostname)-pool`.

## Usage

### Basic Replication
Use a generic pool name; the script resolves it locally via `repl:<hostname>`.
```bash
zfs-replication.sh pool/mydata min1 10
```

### Initial Replication
For the first run (no common snapshots downstream):
```bash
zfs-replication.sh pool/mydata min1 10 --initial
```

### Master Promotion & Recovery
Promotion reorders the `repl:chain` and propagates it. The script uses **Snapshot GUIDs** to ensure consistent ancestry during recovery.

1. **Auto-Discovery (Recommended)**:
   Find the latest snapshot shared by all nodes (matching name AND GUID) and rollback:
   ```bash
   zfs-replication.sh pool/mydata min1 10 --promote --auto [-y]
   ```


2. **Specific Snapshot**:
   Rollback the entire chain to a specific known-good snapshot:
   ```bash
   zfs-replication.sh pool/mydata min1 10 --promote --snap <snapshot_name> [-y]
   ```

3. **Brutal Startover (Dangerous)**:
   Destroy the dataset on all downstream nodes and perform a fresh full send:
   ```bash
   zfs-replication.sh pool/mydata min1 10 --promote --destroy-chain
   ```

### Pause & Resume
To globally pause the Master's replication schedule:
```bash
zfs-replication.sh pool/mydata min1 10 --suspend
```
To resume:
```bash
zfs-replication.sh pool/mydata min1 10 --resume
```
This sets/unsets the `repl:suspend=true` property on all nodes in the chain.

## Cron Integration

Add the same cron job to **all nodes**. Only the current Master will act; others will exit gracefully.
```cron
* * * * * root /usr/local/bin/zfs-replication.sh pool/mydata min1 10 >> /var/log/zfs-replication.log 2>&1
```

## Gotchas & Lessons Learned

1. **Property Propagation**: Custom ZFS properties are passed via base64 arguments during SSH calls because incremental streams do not carry parent dataset changes.
2. **Divergence**: If two nodes act as Master simultaneously, their snapshots will diverge. Use `--promote --auto` to re-synchronize the chain to the last shared state.
3. **Destruction Safety**: Standard resync paths will NOT automatically destroy downstream datasets. The `--destroy-chain` flag is required and guarded by a consistency check.

## Credits
This script incorporates core logic from `zfsbud.sh` by [Pawel Ginalski (gbyte.dev)](https://gbyte.dev).
