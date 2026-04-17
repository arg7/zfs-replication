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
   Configure the Master node's dataset (e.g., `repl:chain=node1,node2,node3`).

## Usage

### Basic Replication
```bash
zfs-replication.sh <dataset> <label> <keep_fallback>
```

### Initial Replication
For the first run (no common snapshots downstream):
```bash
zfs-replication.sh dpool/mydata min1 10 --initial
```

### Master Promotion & Recovery
Promotion reorders the `repl:chain` and propagates it. If the new Master has divergent snapshots, use the following recovery options:

1. **Auto-Discovery (Recommended)**:
   Find the latest snapshot shared by all nodes and rollback the entire chain to it:
   ```bash
   zfs-replication.sh dpool/mydata min1 10 --promote --auto [-y]
   ```

2. **Specific Snapshot**:
   Rollback the entire chain to a specific known-good snapshot:
   ```bash
   zfs-replication.sh dpool/mydata min1 10 --promote --snap <snapshot_name> [-y]
   ```

3. **Brutal Startover (Dangerous)**:
   Destroy the dataset on all downstream nodes and perform a fresh full send:
   ```bash
   zfs-replication.sh dpool/mydata min1 10 --promote --destroy-chain
   ```

### Pause & Resume
To globally pause the Master's replication schedule:
```bash
zfs-replication.sh dpool/mydata min1 10 --suspend
```
To resume:
```bash
zfs-replication.sh dpool/mydata min1 10 --resume
```
This sets/unsets the `repl:suspend=true` property on all nodes in the chain.

## Cron Integration

Add the same cron job to **all nodes**. Only the current Master will act; others will exit gracefully.
```cron
* * * * * root /usr/local/bin/zfs-replication.sh dpool/mydata min1 10 >> /var/log/zfs-replication.log 2>&1
```

## Gotchas & Lessons Learned

1. **Property Propagation**: Custom ZFS properties are passed via base64 arguments during SSH calls because incremental streams do not carry parent dataset changes.
2. **Divergence**: If two nodes act as Master simultaneously, their snapshots will diverge. Use `--promote --auto` to re-synchronize the chain to the last shared state.
3. **Destruction Safety**: Standard resync paths will NOT automatically destroy downstream datasets. The `--destroy-chain` flag is required and guarded by a consistency check.

## Credits
This script incorporates core logic from `zfsbud.sh` by [Pawel Ginalski (gbyte.dev)](https://gbyte.dev).
