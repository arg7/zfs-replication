# ZFS Replication Manager

A robust, cascading ZFS replication script designed for multi-node chains. It handles snapshot creation, incremental transfers with resume support, and graduated retention across the entire chain.

## Features

- **Cascading Replication**: Automatically triggers replication on the next hop in the chain once the local transfer is verified.
- **End-to-End Verification**: Confirms the arrival of the specific snapshot at the final sink before marking local snapshots as "shipped".
- **Automatic Configuration Sync**: All `repl:*` properties (retention, SMTP, chain order) are automatically propagated from the master to all downstream nodes during replication.
- **Master Promotion**: Use the `--promote` flag to promote any node to Master safely. An email notice is sent automatically upon promotion.
- **Cron Safety**: The script can be configured in cron on **all nodes**. It will automatically detect if it is the current Master and only initiate replication if it is at the top of the `repl:chain`.
- **Robust Transfers**: Uses `mbuffer` and `zstd` compression for reliable and fast ZFS send/receive.
- **Graduated Retention**: Different retention policies (keep counts) for each node in the chain.
- **Stuck Job Detection**: Prevents concurrent runs and alerts via SMTP if a job is stuck beyond a timeout.
- **SMTP Alerts**: Sends email notifications for critical failures, stuck jobs, and role changes.

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
   Ensure **Full Mesh** SSH connectivity between all nodes in the chain. Every node must be able to SSH into every other node as the `repl:user` (usually `root`) without a password.
   - Use `ssh-keyscan` to populate `known_hosts` for all node hostnames.
   - Distribute the public key of every node to every other node's `authorized_keys`.

3. **ZFS Properties**:
   Set up the initial configuration on the Master node's dataset:
   ```bash
   zfs set repl:chain=node1,node2,node3 dpool/mydata
   zfs set repl:min1=10,30,90 dpool/mydata
   # ... set SMTP properties ...
   ```

## Configuration (ZFS Properties)

Configuration is stored directly in ZFS properties on the **source dataset**. Once set on the master, these properties propagate to the rest of the chain automatically.

### Required Properties

- `repl:chain`: Comma-separated list of hostnames in the replication order.
- `repl:user`: SSH user for connecting to the next hop (defaults to `root`).
- `repl:<label>`: Comma-separated keep counts (e.g., `repl:min1=10,30,90`).

### Alerting & Safety

- `repl:timeout`: Seconds before a job is considered stuck (defaults to 3600).
- `repl:smtp_host`, `repl:smtp_port`, `repl:smtp_user`, `repl:smtp_password`, `repl:smtp_from`, `repl:smtp_to`, `repl:smtp_protocol`.

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

### Master Promotion
To promote the current node to Master:
```bash
zfs-replication.sh dpool/mydata min1 10 --promote
```
This updates the `repl:chain` on the current node and triggers a replication to update the rest of the chain.

## Cron Integration

You can safely add the same cron job to **all nodes** in your cluster.

Example `/etc/cron.d/zfs-repl`:
```cron
* * * * * root /usr/local/bin/zfs-replication.sh dpool/mydata min1 10 >> /var/log/zfs-replication.log 2>&1
```

- **Master Node**: Will execute the full snapshot and replication cycle.
- **Downstream Nodes**: Will check if they are Master, see that they are not, and exit immediately with `INFO: Node is not Master. Skipping initiation (Cron safety).`.
- **After Promotion**: If you promote `node2`, its cron job will automatically start initiating replication on the next minute, while `node1` will automatically become passive.

## Monitoring & Troubleshooting

- **Logs**: Check `/var/log/syslog` for `zfs-auto-snapshot` activity.
- **Verification**: Check the `zfs-send:shipped` property on snapshots:
  ```bash
  zfs list -t snap -o name,zfs-send:shipped -r dpool/mydata
  ```
- **Lock Files**: If a job is interrupted, check `/tmp/<dataset>-<label>.lock`.
- **Manual Test**: You can run the script manually from any node. Use the `--mark-only` flag to test rotation and retention without triggering a new send.

## Gotchas & Lessons Learned

1. **Hostname Matching**: The hostnames in `repl:chain` must exactly match the output of the `hostname` command on each node.
2. **Dataset Paths**: The script automatically maps between local pool names (e.g., `node1-pool` vs `node2-pool`) assuming the dataset structure below the pool is identical.
3. **Property Propagation**: Custom ZFS properties (`repl:*`) are passed via base64 arguments during the SSH cascade because incremental ZFS streams do not carry parent dataset property changes.
4. **SSH Paths**: Ensure the script is in the same absolute path (or symlinked into `/usr/local/bin`) on all nodes.

## Credits
This script incorporates core logic from `zfsbud.sh` by [Pawel Ginalski (gbyte.dev)](https://gbyte.dev).
