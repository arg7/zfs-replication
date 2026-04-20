# Zeplicator: Docker Test Bench & Debugging Notes

This document describes the simulated multi-node environment used to verify Zeplicator's cascading replication and master promotion logic.

---

## 1. Test Bench Configuration
We simulate a three-node production chain using Docker on a single ZFS-capable host.

### Infrastructure
*   **Containers:** 3 nodes (`node1`, `node2`, `node3`) running Ubuntu 22.04.
*   **Storage:** 
    *   3 distinct ZFS pools: `node1-pool`, `node2-pool`, `node3-pool`.
    *   Backed by 8GB sparse image files on the host (`/root/zfs-dev/node[x].img`).
*   **Mounting:** The host directory `/root/zfs-dev` is bind-mounted to `/scripts` on all containers.
*   **Networking:**
    *   `node1`: 172.17.0.2 (Master)
    *   `node2`: 172.17.0.3 (Middle)
    *   `node3`: 172.17.0.4 (Sink)
    *   **SSH:** Full mesh connectivity enabled; `StrictHostKeyChecking` disabled for seamless automation.

### Dataset Topology
Each node uses a uniquely named dataset to test path-mapping robustness:
*   `node1-pool/data1` -> `node2-pool/data2` -> `node3-pool/data3`

### Step-by-step Setup (Ubuntu Host)
Run the following commands on your Ubuntu host to replicate this environment:

```bash
# 1. Install prerequisites
sudo apt update && sudo apt install -y zfsutils-linux docker.io

# 2. Create sparse images for virtual disks (8GB each)
truncate -s 8G node1.img
truncate -s 8G node2.img
truncate -s 8G node3.img

# 3. Create ZFS pools using the image files
sudo zpool create node1-pool $(pwd)/node1.img
sudo zpool create node2-pool $(pwd)/node2.img
sudo zpool create node3-pool $(pwd)/node3.img

# 4. Create the datasets
sudo zfs create node1-pool/data1
sudo zfs create node2-pool/data2
sudo zfs create node3-pool/data3

# 5. Launch containers and install dependencies
for i in {1..3}; do
  docker run -d --name node${i} \
    --privileged \
    -v /dev/zfs:/dev/zfs \
    -v $(pwd):/scripts \
    ubuntu:22.04 sleep infinity
  
  docker exec node${i} apt update
  docker exec node${i} apt install -y zfsutils-linux openssh-server openssh-client mbuffer zstd curl iproute2
done

# 6. Configure SSH Full Mesh Connectivity
# Generate keys, start SSH service, and distribute keys across all nodes
for i in {1..3}; do
  docker exec node${i} bash -c 'ssh-keygen -t rsa -N "" -f ~/.ssh/id_rsa'
  docker exec node${i} bash -c 'mkdir -p /run/sshd && /usr/sbin/sshd'
  docker exec node${i} bash -c 'echo "StrictHostKeyChecking no" >> ~/.ssh/config && chmod 600 ~/.ssh/config'
done

# Distribute keys to build the mesh
for i in {1..3}; do
  PUB_KEY=$(docker exec node${i} cat /root/.ssh/id_rsa.pub)
  for j in {1..3}; do
    docker exec node${j} bash -c "echo '$PUB_KEY' >> ~/.ssh/authorized_keys"
  done
done

# 7. Apply Zeplicator Configuration
# Create a configuration file with the chain topology, IPs, and retention rules
cat << 'EOF' > test-bench.conf
chain                          node1,node2,node3
node:node1:fqdn                172.17.0.2
node:node1:fs                  node1-pool/data1
node:node2:fqdn                172.17.0.3
node:node2:fs                  node2-pool/data2
node:node3:fqdn                172.17.0.4
node:node3:fs                  node3-pool/data3
role:master:keep:min1          10
role:middle:keep:min1          30
role:sink:keep:min1            90
smtp_from                      zeplicator@acme.com
smtp_host                      mail.acme.com
smtp_password                  [smtp password]
smtp_port                      465
smtp_protocol                  smtps
smtp_starttls                  false
smtp_to                        sysadmin@acme.com
smtp_user                      [smtp user]
user                           root
EOF

# Import the configuration into the master dataset
# (Note: Zeplicator automatically translates shorthand keys to 'repl:*' properties)
docker exec node1 /scripts/zeplicator-standalone.sh node1-pool/data1 --config --import /scripts/test-bench.conf
```

### Data Load (IO Simulation)
A background process on `node1` provides constant incremental changes:
```bash
while true; do date >> /node1-pool/data1/test.txt; sleep 1; done
```

---

## 2. Major Gotchas Encountered

### A. ZFS "Ghost" Visibility (Shared Kernel Trap)
*   **The Problem:** Since Docker containers share the host kernel, every container could "see" all three ZFS pools in `zpool list`. The script's original snapshot discovery used global `grep` commands, causing `node1` to accidentally find and attempt to process snapshots belonging to `node2`.
*   **The Fix:** Updated all discovery logic to use explicit recursive scoping (`zfs list -r <dataset>`) to ensure nodes only see their own intended data.

### B. Bash "local" Syntax Errors
*   **The Problem:** The script used the `local` keyword for variable declarations in the main execution body (global scope). This is illegal in Bash and caused variable assignments to fail or scripts to crash.
*   **The Fix:** Stripped `local` from the main orchestrator flow and ensured it is only used inside function definitions.

### C. False-Positive GUID Matching
*   **The Problem:** When a downstream node had an empty dataset, the GUID check compared two empty strings (or two `-` characters). Bash evaluated `"" == ""` as true, leading the script to believe a common snapshot existed and attempt a rollback/incremental send to a non-existent target.
*   **The Fix:** Implemented a sanity check to explicitly ignore empty or null GUIDs during the comparison loop.

### D. Recursive Path Appending
*   **The Problem:** The script originally forced a "parent/child" relationship by appending the source leaf name to the target pool (e.g., `target-pool/source-data`).
*   **The Fix:** Modified logic to treat `repl:node:<alias>:fs` as a **full literal path** if it contains a `/`, allowing for heterogeneous dataset naming across the chain.

### E. Restrictive PATH Environment
*   **The Problem:** The inherited `zfsbud` logic reset `PATH` to a minimal set (`/usr/bin:/sbin:/bin`), which broke standard utilities like `date`, `grep`, and `readlink` inside the Ubuntu containers.
*   **The Fix:** Expanded `zbud_PATH` in `zfs-transfer.lib.sh` to include `/usr/local/bin` and `/usr/local/sbin`.

---

## 3. Tomorrow's Focus: Resilience Testing

1.  **Divergence Recovery:** Manually creating "rogue" snapshots on downstream nodes to verify the `--promote --auto` healing mechanism.
2.  **Split-Brain Prevention:** Implementing a "Master Election" check to prevent two nodes from acting as Master simultaneously if the `repl:chain` is updated inconsistently.
3.  **Promotion Dry-Runs:** Adding a safety flag to show planned rollbacks before they are executed.
