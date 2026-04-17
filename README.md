# ZFS Replication Manager

A robust, cascading ZFS replication script designed for multi-node chains. It handles snapshot creation, incremental transfers with resume support, and graduated retention across the entire chain.

## Features

- **Cascading Replication**: Automatically triggers replication on the next hop in the chain once the local transfer is verified.
- **End-to-End Verification**: Confirms the arrival of the specific snapshot at the final sink before marking local snapshots as "shipped".
- **Robust Transfers**: Uses `zfsbud` logic with `mbuffer` and `zstd` compression for reliable and fast ZFS send/receive.
- **Graduated Retention**: Different retention policies (keep counts) for each node in the chain.
- **Stuck Job Detection**: Prevents concurrent runs and alerts via SMTP if a job is stuck beyond a timeout.
- **SMTP Alerts**: Sends email notifications for critical failures and stuck jobs.

## Dependencies

The following packages must be installed on all nodes in the replication chain:

- `zfsutils-linux`
- `zfs-auto-snapshot`
- `openssh-server` / `openssh-client`
- `mbuffer`
- `zstd`
- `curl` (for SMTP alerts)

## Installation

1. Clone the repository.
2. Link the script to your PATH:
   ```bash
   ln -s $(pwd)/zfs-replication.sh /usr/local/bin/zfs-replication.sh
   ```

## Configuration (ZFS Properties)

Configuration is stored directly in ZFS properties on the **source dataset**.

### Required Properties

- `repl:chain`: Comma-separated list of hostnames in the replication order.
  - Example: `zfs set repl:chain=node1,node2,node3 dpool/mydata`
- `repl:user`: SSH user for connecting to the next hop (defaults to `root`).
  - Example: `zfs set repl:user=backupuser dpool/mydata`

### Retention Properties

Set the number of snapshots to keep for a specific label, corresponding to the nodes in the chain.
- `repl:<label>`: Comma-separated keep counts.
  - Example: `zfs set repl:min1=10,30,90 dpool/mydata` (Keep 10 on node1, 30 on node2, 90 on node3).

### Alerting & Safety

- `repl:timeout`: Seconds before a job is considered stuck (defaults to 3600).
- `repl:smtp_host`: SMTP server address.
- `repl:smtp_port`: SMTP server port (defaults to 465).
- `repl:smtp_user`: SMTP username.
- `repl:smtp_password`: SMTP password.
- `repl:smtp_from`: Sender email address.
- `repl:smtp_to`: Recipient email address.
- `repl:smtp_protocol`: `smtps` (default) or `smtp`.

## Usage

### Basic Replication
Run on the master node (the first node in the chain):
```bash
zfs-replication.sh <dataset> <label> <keep_fallback>
```
Example:
```bash
zfs-replication.sh dpool/mydata min1 10
```

### Initial Replication
For the first-ever run (when no common snapshots exist on downstream nodes), use the `--initial` flag:
```bash
zfs-replication.sh dpool/mydata min1 10 --initial
```

### Maintenance
To purge old shipped snapshots without performing a new replication:
```bash
zfs-replication.sh dpool/mydata min1 10 --mark-only
```

## Credits
This script incorporates core logic from `zfsbud.sh` by [Pawel Ginalski (gbyte.dev)](https://gbyte.dev).
