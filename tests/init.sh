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

ZEP_BIN="$SCRIPT_DIR/../build/zep"

# ZEP Properties from conf or defaults
if [[ -z "$CHAIN" ]]; then
    CHAIN=$(seq -s, 1 "$NUM_NODES" | sed 's/[0-9]*/node&/g')
fi
POLICY=${POLICY:-fail}
SMTP_FROM=${SMTP_FROM}
SMTP_HOST=${SMTP_HOST}
SMTP_PASSWORD=${SMTP_PASSWORD}
SMTP_PORT=${SMTP_PORT}
SMTP_PROTOCOL=${SMTP_PROTOCOL}
SMTP_STARTTLS=${SMTP_STARTTLS}
SMTP_TO=${SMTP_TO}
SMTP_USER=${SMTP_USER}
ZFS_FORCE=${ZFS_FORCE:-false}
ZFS_RATE=${ZFS_RATE:-1M}

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

    # Import configuration using zep
    echo "Importing configuration to $DATASET_NAME using zep..."
    "$ZEP_BIN" "$DATASET_NAME" --alias node1 --config --import "$CONFIG_FILE"

    echo "Successfully created and configured $DATASET_NAME"
done

echo "Initialization complete."
