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

echo "Cleaning up $NUM_NODES nodes..."

for i in $(seq 1 "$NUM_NODES"); do
    POOL_NAME="zep-node-$i"
    IMG_FILE="/tmp/$POOL_NAME.img"
    DNS_NAME="zep-node-$i.local"

    echo "Cleaning up $POOL_NAME..."

    # Destroy ZFS pool if it exists
    if zpool list "$POOL_NAME" >/dev/null 2>&1; then
        echo "  Destroying pool $POOL_NAME..."
        zpool destroy "$POOL_NAME"
    fi

    # Delete sparse image file
    if [[ -f "$IMG_FILE" ]]; then
        echo "  Removing image file $IMG_FILE..."
        rm -f "$IMG_FILE"
    fi

    # Remove DNS entry from /etc/hosts
    if grep -q "$DNS_NAME" /etc/hosts; then
        echo "  Removing $DNS_NAME from /etc/hosts..."
        # Use sed to delete the line containing the DNS name
        sed -i "/$DNS_NAME/d" /etc/hosts
    fi
done

# Remove the master config file
CONFIG_FILE="/tmp/zep-master.conf"
if [[ -f "$CONFIG_FILE" ]]; then
    echo "Removing temporary config file $CONFIG_FILE..."
    rm -f "$CONFIG_FILE"
fi

echo "Cleanup complete."
