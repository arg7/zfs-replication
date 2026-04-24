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
zfs:rate=$ZFS_RATE
EOF

# Add node definitions to master config
for j in $(seq 1 "$NUM_NODES"); do
    echo "node:node$j:fqdn=zep-node-$j.local" >> "$CONFIG_FILE"
    echo "node:node$j:fs=zep-node-$j/test-$j" >> "$CONFIG_FILE"
done

echo "Initializing $NUM_NODES nodes with $POOL_SIZE pools..."

for i in $(seq 1 "$NUM_NODES"); do
    POOL_NAME="zep-node-$i"
    IMG_FILE="/tmp/$POOL_NAME.img"
    DATASET_NAME="$POOL_NAME/test-$i"

    echo "Setting up $POOL_NAME..."

    # Create sparse file
    truncate -s "$POOL_SIZE" "$IMG_FILE"

    # Create ZFS pool
    if zpool list "$POOL_NAME" >/dev/null 2>&1; then
        echo "Pool $POOL_NAME already exists, destroying it..."
        zpool destroy "$POOL_NAME"
    fi
    
    zpool create "$POOL_NAME" "$IMG_FILE"

    # Create dataset
    zfs create "$DATASET_NAME"

    # DNS Registration
    DNS_NAME="zep-node-$i.local"
    if ! grep -q "$DNS_NAME" /etc/hosts; then
        echo "Registering $DNS_NAME to 127.0.0.1 in /etc/hosts"
        echo "127.0.0.1 $DNS_NAME" >> /etc/hosts
    else
        echo "DNS entry for $DNS_NAME already exists."
    fi

    # Add to known_hosts
    if ! ssh-keygen -F "$DNS_NAME" >/dev/null 2>&1; then
        echo "Adding $DNS_NAME to ~/.ssh/known_hosts"
        ssh-keyscan -H "$DNS_NAME" >> ~/.ssh/known_hosts 2>/dev/null
    else
        echo "$DNS_NAME is already in known_hosts."
    fi

    echo "Successfully created $DATASET_NAME"
done

# Import configuration once on the Master node
MASTER_DATASET="zep-node-1/test-1"
echo "Importing configuration to $MASTER_DATASET using zep..."
"$ZEP_BIN" "$MASTER_DATASET" --alias node1 --config --import "$CONFIG_FILE"

echo "Initialization complete."
