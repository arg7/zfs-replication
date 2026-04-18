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
- **Peer-to-Peer Healing**: If a node lacks a common snapshot with its target, it automatically searches the chain for a "Donor" node (like a Sink) to delegate a peer-to-peer transfer.
- **Divergence Detection**: Before performing a forced rollback or receive, the script uses `zfs diff` to identify and log local changes made on the destination since the last sync.
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
| `repl:chain` | **(Required)** Comma-separated list of host aliases. | `node1,node2,node3` |
| `repl:node:<alias>:fs` | Physical pool name for a specific host alias. | `repl:node:node1:fs=tank` |
| `repl:node:<alias>:fqdn` | Real address/FQDN for a host alias. | `repl:node:node1:fqdn=10.0.0.5` |
| `repl:node:<alias>:user` | SSH user for a host alias. | `repl:node:node1:user=repluser` |
| `repl:node:<alias>:keep:<label>` | Host-specific retention (highest priority). | `repl:node:node1:keep:min1=30` |
| `repl:role:<role>:keep:<label>` | Role-based retention (roles: `master`, `middle`, `sink`). | `repl:role:sink:keep:min1=90` |
| `repl:user` | **(Global)** Fallback SSH user for replication. | `root` |

### Node Configuration & Aliases
The script uses a namespaced configuration system. This allows you to use short aliases in the `repl:chain` while managing real connectivity details separately:
1. **Alias resolution**: The script finds the alias in `repl:chain`.
2. **FQDN**: Looks for `repl:node:<alias>:fqdn`, defaults to alias.
3. **User**: Looks for `repl:node:<alias>:user`, defaults to global `repl:user`, then `root`.
4. **Pool/FS**: Looks for `repl:node:<alias>:fs`, defaults to `pool`, then `$(alias)-pool`.
5. **Retention**: Determining the "keep count" follows this priority:
   - `repl:node:<alias>:keep:<label>` (Host-specific)
   - `repl:role:<role>:keep:<label>` (Role-specific based on chain position)
   - Command line fallback argument.


## Local Cluster Test Bench (for Development)

For streamlined development and testing, you can simulate a multi-node ZFS replication chain on a single host using aliases and `localhost`.

### Setup Steps

1.  **Prepare Host Environment**:
    Ensure `zfs-auto-snapshot`, `mbuffer`, `zstd`, `curl`, and `openssh-server`/`client` are installed on your host machine. Configure passwordless SSH to `localhost`.
    ```bash
    sudo apt-get update && sudo apt-get install -y zfs-auto-snapshot mbuffer zstd curl openssh-server openssh-client
    mkdir -p ~/.ssh
    [[ -f ~/.ssh/id_ed25519 ]] || ssh-keygen -t ed25519 -f ~/.ssh/id_ed25519 -N ""
    cat ~/.ssh/id_ed25519.pub >> ~/.ssh/authorized_keys
    chmod 600 ~/.ssh/authorized_keys
    ssh-keyscan -H localhost >> ~/.ssh/known_hosts
    ```

2.  **Create Local Zpools (File-backed)**:
    Create three file-backed ZFS pools on your host. These will act as `local1`, `local2`, and `local3`.
    ```bash
    for i in 1 2 3; do
        truncate -s 1G "test_node${i}.img"
        sudo zpool create -f "local${i}" "$(pwd)/test_node${i}.img"
    done
    ```

3.  **Configure Master Dataset Properties**:
    Create a master dataset (e.g., `local1/data`) and configure it with the `repl:chain` and node-specific properties. All aliases (`node1`, `node2`, `node3`) will point to `localhost`.
    ```bash
    sudo zfs create local1/data
    sudo zfs set repl:chain=node1,node2,node3 local1/data
    sudo zfs set repl:node:node1:fqdn=localhost repl:node:node1:fs=local1 local1/data
    sudo zfs set repl:node:node2:fqdn=localhost repl:node:node2:fs=local2 local1/data
    sudo zfs set repl:node:node3:fqdn=localhost repl:node:node3:fs=local3 local1/data
    sudo zfs set repl:role:master:keep:min1=10 repl:role:middle:keep:min1=30 repl:role:sink:keep:min1=90 local1/data
    ```

### Running Tests

To simulate running the script as `node1`, `node2`, or `node3`, use the `REPL_ME` environment variable:

*   **As Node1 (Master)**:
    ```bash
    REPL_ME=node1 ./zfs-replication.sh local1/data min1 10 --initial
    # Subsequent runs
    REPL_ME=node1 ./zfs-replication.sh local1/data min1 10
    ```

*   **As Node2 (Middle)**:
    ```bash
    REPL_ME=node2 ./zfs-replication.sh local2/data min1 10
    ```

*   **As Node3 (Sink)**:
    ```bash
    REPL_ME=node3 ./zfs-replication.sh local3/data min1 10
    ```

This setup allows for rapid iteration and debugging of complex replication scenarios on a single machine.

### Cleanup

To destroy the local test pools and image files:
```bash
for i in 1 2 3; do
    sudo zpool destroy "local${i}"
    rm "test_node${i}.img"
done
```


## Gotchas & Lessons Learned

1. **Property Propagation**: Custom ZFS properties are passed via base64 arguments during SSH calls because incremental streams do not carry parent dataset changes.
2. **Divergence**: If two nodes act as Master simultaneously, their snapshots will diverge. Use `--promote --auto` to re-synchronize the chain to the last shared state.
3. **Destruction Safety**: Standard resync paths will NOT automatically destroy downstream datasets. The `--destroy-chain` flag is required and guarded by a consistency check.

## Credits
This script incorporates core logic from `zfsbud.sh` by [Pawel Ginalski (gbyte.dev)](https://gbyte.dev).
