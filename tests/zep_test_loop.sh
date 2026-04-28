#!/bin/bash

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
ZEP_BIN=$(command -v zep)
DS="zep-node-1/test-1"
LABEL="min1"

# Clean up stale tmp files from previous runs
rm -rf /tmp/zep_* 2>/dev/null || true

pause_if_unexpected() {
    local expected=$1
    local actual=$2
    local context=$3

    if [ "$expected" = "zero" ] && [ "$actual" -ne 0 ]; then
        echo "UNEXPECTED Exit Code: $actual for $context. Expected: 0 (Success)."
        read -p "Press Enter to continue or Ctrl+C to abort..."
    elif [ "$expected" = "nonzero" ] && [ "$actual" -eq 0 ]; then
        echo "UNEXPECTED Exit Code: $actual for $context. Expected: non-zero (Failure)."
        read -p "Press Enter to continue or Ctrl+C to abort..."
    fi
}

if false; then
echo "=== Zeplicator Activity Loop ==="
echo "1. Performing Initial Sync..."
$ZEP_BIN --alias node1 "$DS" "$LABEL" --init
EXIT_CODE=$?
echo "Initial Sync Exit Code: $EXIT_CODE"
pause_if_unexpected "zero" "$EXIT_CODE" "Initial Sync"
fi

echo ""
echo "2. Starting periodic sync loop (Master vs Non-Master)..."
while true; do 
    echo "----------------------------------------"
    echo "[$(date +%T)] Master Sync (node1)..."
    $ZEP_BIN --alias node1 "$DS" "$LABEL"
    EXIT_CODE=$?
    echo "Master Sync Exit Code: $EXIT_CODE"
    pause_if_unexpected "zero" "$EXIT_CODE" "Master Sync"
    
    echo "----------------------------------------"
    echo "[$(date +%T)] Non-Master Sync (node2) - Expected to fail..."
    $ZEP_BIN --alias node2 "$DS" "$LABEL"
    EXIT_CODE=$?
    echo "Non-Master Sync Exit Code: $EXIT_CODE"
    pause_if_unexpected "nonzero" "$EXIT_CODE" "Non-Master Sync"
    
    echo "----------------------------------------"
    echo "Sleeping 3s..."
    sleep 3
done