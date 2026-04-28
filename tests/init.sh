#!/bin/bash
set -e

# Load configuration
SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
CONF_FILE="$SCRIPT_DIR/test.conf"

if [[ -f "$CONF_FILE" ]]; then
    source "$CONF_FILE"
else
    echo "Error: $CONF_FILE not found."
    exit 1
fi

NUM_NODES=${NUM_NODES:-3}
POOL_SIZE=${POOL_SIZE:-128M}
RAMDISK="/tmp/zep-ramdisk"

# --- Ramdisk setup ---
# Convert POOL_SIZE to bytes (handles suffixes like 128M, 256M, 1G)
pool_bytes=$(numfmt --from=iec "$POOL_SIZE")
ramdisk_size=$(( pool_bytes * NUM_NODES + pool_bytes / 2 ))  # pool_size * nodes + 50% headroom

if mountpoint -q "$RAMDISK" 2>/dev/null; then
    echo "Ramdisk already mounted at $RAMDISK"
else
    echo "Creating ${ramdisk_size}-byte ramdisk at $RAMDISK..."
    mkdir -p "$RAMDISK"
    mount -t tmpfs -o size="${ramdisk_size}" tmpfs "$RAMDISK"
    echo "Ramdisk mounted at $RAMDISK"
fi

# Ensure build/zep is ready
echo "Building zep..."
make -C "$SCRIPT_DIR/.." > /dev/null

# Dependency Check
SYSTEM_DEPS=("zfs" "zpool" "ssh" "mbuffer" "zstd" "curl" "tmux")
PROJECT_DEPS=("zep" "iomon")
MISSING_SYSTEM=()
MISSING_PROJECT=()

for dep in "${SYSTEM_DEPS[@]}"; do
    if ! command -v "$dep" >/dev/null 2>&1; then
        MISSING_SYSTEM+=("$dep")
    fi
done

for dep in "${PROJECT_DEPS[@]}"; do
    if ! command -v "$dep" >/dev/null 2>&1; then
        MISSING_PROJECT+=("$dep")
    fi
done

if [ ${#MISSING_SYSTEM[@]} -ne 0 ] || [ ${#MISSING_PROJECT[@]} -ne 0 ]; then
    if [ ${#MISSING_SYSTEM[@]} -ne 0 ]; then
        echo "Error: Missing system dependencies: ${MISSING_SYSTEM[*]}"
        echo "Please install them (e.g., sudo apt install zfsutils-linux openssh-client mbuffer zstd curl tmux)"
    fi
    if [ ${#MISSING_PROJECT[@]} -ne 0 ]; then
        echo "Error: Missing project binaries: ${MISSING_PROJECT[*]}"
        echo "Please run: sudo make install"
    fi
    exit 1
fi

ZEP_BIN=$(command -v zep)

# ZEP Properties from conf or defaults
CHAIN=$(seq 1 "$NUM_NODES" | sed 's/^/node/' | paste -sd, -)
POLICY=${POLICY:-fail}
ZFS_FORCE=${ZFS_FORCE:-false}
ZFS_RAW=${ZFS_RAW:-false}
ZFS_RESUME=${ZFS_RESUME:-false}
ZFS_RATE=${ZFS_RATE:-1M}
ALERT_CRITICAL_THRESHOLD=${ALERT_CRITICAL_THRESHOLD:-0}
ALERT_WARN_THRESHOLD=${ALERT_WARN_THRESHOLD:-0}
ALERT_INFO_THRESHOLD=${ALERT_INFO_THRESHOLD:-0}

# Generate master config file
CONFIG_FILE="/tmp/zep-master.conf"
echo "Generating $CONFIG_FILE..."
cat <<EOF > "$CONFIG_FILE"
chain=$CHAIN
debug:send_delay=0
force=true
policy=$POLICY
role:master:keep:min1=10
role:middle:keep:min1=30
role:sink:keep:min1=90
smtp:from=$SMTP_FROM
smtp:host=$SMTP_HOST
smtp:password=$SMTP_PASSWORD
smtp:port=$SMTP_PORT
smtp:protocol=$SMTP_PROTOCOL
smtp:starttls=$SMTP_STARTTLS
smtp:to=$SMTP_TO
smtp:user=$SMTP_USER
alert:critical:threshold=$ALERT_CRITICAL_THRESHOLD
alert:warn:threshold=$ALERT_WARN_THRESHOLD
alert:info:threshold=$ALERT_INFO_THRESHOLD
suspend=false
user=root
zfs:force=$ZFS_FORCE
zfs:raw=$ZFS_RAW
zfs:resume=$ZFS_RESUME
zfs:throttle=$ZFS_THROTTLE
EOF

# Add node definitions to master config
# Master (node1) uses root, chain nodes use dedicated zep-user accounts
for j in $(seq 1 "$NUM_NODES"); do
    echo "node:node$j:fqdn=zep-node-$j.local" >> "$CONFIG_FILE"
    echo "node:node$j:fs=zep-node-$j/test-$j" >> "$CONFIG_FILE"
    if [[ $j -ne 1 ]]; then
        echo "node:node$j:user=zep-user-$j" >> "$CONFIG_FILE"
    fi
done

echo "Initializing $NUM_NODES nodes with $POOL_SIZE pools..."

for i in $(seq 1 "$NUM_NODES"); do
    POOL_NAME="zep-node-$i"
    IMG_FILE="$RAMDISK/$POOL_NAME.img"
    DATASET_NAME="$POOL_NAME/test-$i"

    echo "Setting up $POOL_NAME..."

    # Create sparse file on ramdisk
    truncate -s "$POOL_SIZE" "$IMG_FILE"

    # Create ZFS pool
    if zpool list "$POOL_NAME" >/dev/null 2>&1; then
        echo "Pool $POOL_NAME already exists, destroying it..."
        zpool destroy "$POOL_NAME"
    fi

    zpool create "$POOL_NAME" "$IMG_FILE"

    # Create dataset only on master (node1); chain nodes pre-created for zfs recv
    zfs create "$DATASET_NAME"
    if [[ $i -ne 1 ]]; then
        zfs set canmount=noauto "$DATASET_NAME"
        zfs unmount "$DATASET_NAME" 2>/dev/null || true
    fi
    echo "Successfully created $DATASET_NAME"

    # Create dedicated replication user for all nodes with minimal ZFS rights
    ZEP_USER="zep-user-$i"
    echo "  Setting up $ZEP_USER with minimal ZFS rights..."

    # Create user if not exists
    if ! id "$ZEP_USER" >/dev/null 2>&1; then
        useradd -m -s /bin/bash "$ZEP_USER" 2>/dev/null || \
        adduser --disabled-password --gecos "" "$ZEP_USER" 2>/dev/null || true
    fi

    # Generate SSH key for the zep-user if not already present
    ZEP_HOME=$(getent passwd "$ZEP_USER" | cut -d: -f6)
    [[ -z "$ZEP_HOME" ]] && ZEP_HOME="/home/$ZEP_USER"
    ZEP_SSH_DIR="$ZEP_HOME/.ssh"
    mkdir -p "$ZEP_SSH_DIR"
    if [[ ! -f "$ZEP_SSH_DIR/id_rsa" ]]; then
        ssh-keygen -t rsa -b 2048 -f "$ZEP_SSH_DIR/id_rsa" -N "" -q
    fi
    # Start authorized_keys with root's pubkey so master can connect
    if [[ -f /root/.ssh/id_rsa.pub ]]; then
        cp /root/.ssh/id_rsa.pub "$ZEP_SSH_DIR/authorized_keys"
    fi
    chown -R "$ZEP_USER:$ZEP_USER" "$ZEP_SSH_DIR"
    chmod 700 "$ZEP_SSH_DIR"
    chmod 600 "$ZEP_SSH_DIR/id_rsa" "$ZEP_SSH_DIR/authorized_keys"
    chmod 644 "$ZEP_SSH_DIR/id_rsa.pub"

    # Delegate minimal ZFS permissions for replication
    # Pool-level: create+mount needed for zfs recv to create/receive datasets
    zfs allow "$ZEP_USER" create,mount "$POOL_NAME"
    # Dataset-level: minimal set for send/receive pipeline
    zfs allow "$ZEP_USER" send,receive,snapshot,hold,release,userprop "$DATASET_NAME"

    echo "  ✅ $ZEP_USER set up with delegated ZFS rights on $POOL_NAME"
done

# Full mesh SSH: all zep-users can reach each other (needed for pre-flight + master rotation)
echo "Setting up full-mesh SSH auth between zep-users..."
for i in $(seq 1 "$NUM_NODES"); do
    ZEP_HOME=$(getent passwd "zep-user-$i" | cut -d: -f6)
    [[ -z "$ZEP_HOME" ]] && ZEP_HOME="/home/zep-user-$i"
    for j in $(seq 1 "$NUM_NODES"); do
        [[ $i -eq $j ]] && continue
        JHOME=$(getent passwd "zep-user-$j" | cut -d: -f6)
        [[ -z "$JHOME" ]] && JHOME="/home/zep-user-$j"
        cat "$JHOME/.ssh/id_rsa.pub" >> "$ZEP_HOME/.ssh/authorized_keys" 2>/dev/null || true
    done
    # Also add zep-user's pubkey to root's authorized_keys
    cat "$ZEP_HOME/.ssh/id_rsa.pub" >> /root/.ssh/authorized_keys 2>/dev/null || true
done

# Populate known_hosts for all zep-users (copy from root's known_hosts)
echo "Populating SSH known_hosts for zep-users..."
for i in $(seq 1 "$NUM_NODES"); do
    ZEP_USER="zep-user-$i"
    ZEP_HOME=$(getent passwd "$ZEP_USER" | cut -d: -f6)
    [[ -z "$ZEP_HOME" ]] && ZEP_HOME="/home/$ZEP_USER"
    ZEP_SSH_DIR="$ZEP_HOME/.ssh"
    cp /root/.ssh/known_hosts "$ZEP_SSH_DIR/known_hosts" 2>/dev/null || true
    chown "$ZEP_USER:$ZEP_USER" "$ZEP_SSH_DIR/known_hosts"
    chmod 600 "$ZEP_SSH_DIR/known_hosts"
done

# Clean up stale temp files from previous runs that might block non-root access
rm -rf /tmp/zep_* 2>/dev/null || true

# Import configuration once on the Master node
MASTER_DATASET="zep-node-1/test-1"
echo "Importing configuration to $MASTER_DATASET using zep..."
"$ZEP_BIN" "$MASTER_DATASET" --alias node1 --config --import "$CONFIG_FILE"

echo ""
echo "=== Initialization complete ==="
echo "Pools on ramdisk: $RAMDISK"
echo "To clean up: bash $SCRIPT_DIR/done.sh"
