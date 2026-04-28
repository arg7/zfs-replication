#!/bin/bash
set -e

# Load configuration
SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
CONF_FILE="$SCRIPT_DIR/test.conf"
RAMDISK="/tmp/zep-ramdisk"

if [[ -f "$CONF_FILE" ]]; then
    source "$CONF_FILE"
fi

NUM_NODES=${NUM_NODES:-3}

echo "Tearing down test environment..."

# Destroy ZFS pools
for i in $(seq 1 "$NUM_NODES"); do
    POOL_NAME="zep-node-$i"
    if zpool list "$POOL_NAME" >/dev/null 2>&1; then
        echo "Destroying pool $POOL_NAME..."
        zpool destroy "$POOL_NAME"
    fi
done

# Unmount ramdisk
if mountpoint -q "$RAMDISK" 2>/dev/null; then
    echo "Unmounting ramdisk at $RAMDISK..."
    umount "$RAMDISK"
    rmdir "$RAMDISK"
    echo "Ramdisk unmounted."
else
    echo "No ramdisk found at $RAMDISK, skipping."
fi

# Clean up temp files
rm -f /tmp/zep-master.conf

# Clean up /etc/hosts entries
for i in $(seq 1 "$NUM_NODES"); do
    DNS_NAME="zep-node-$i.local"
    if grep -q "$DNS_NAME" /etc/hosts 2>/dev/null; then
        echo "Removing $DNS_NAME from /etc/hosts..."
        sed -i "/$DNS_NAME/d" /etc/hosts
    fi
done

# Remove zep-user accounts created for all nodes
for i in $(seq 1 "$NUM_NODES"); do
    ZEP_USER="zep-user-$i"
    if id "$ZEP_USER" >/dev/null 2>&1; then
        echo "  Removing user $ZEP_USER..."
        userdel -r "$ZEP_USER" 2>/dev/null || \
        deluser --remove-home "$ZEP_USER" 2>/dev/null || true
    fi
done

# Remove the master config file
CONFIG_FILE="/tmp/zep-master.conf"
if [[ -f "$CONFIG_FILE" ]]; then
    echo "Removing temporary config file $CONFIG_FILE..."
    rm -f "$CONFIG_FILE"
fi

# Clean up zep log and temp files
rm -rf /tmp/zep_* 2>/dev/null || true

# Remove zep rotation cron entries
crontab -l 2>/dev/null | grep -v 'zep.*--rotate' | crontab - 2>/dev/null || true

echo "=== Cleanup complete ==="
