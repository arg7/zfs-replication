#!/bin/bash
# Test split-brain detection, resilience mode recovery, and rollback
#
# Phase 1: Write data to down-chain node → zfsbud detects divergence → exit 2
# Phase 2: Set policy=resilience on master → replication skips failing node → exit 0
# Phase 3: Rollback down-chain node to latest snap → divergence erased → replication works

set -uo pipefail

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
[[ -f "$SCRIPT_DIR/test.conf" ]] && source "$SCRIPT_DIR/test.conf"

NUM_NODES=${NUM_NODES:-3}
ZEP="$SCRIPT_DIR/../build/zep"

NODES=()
for (( i=1; i<=NUM_NODES; i++ )); do NODES+=("$i"); done

PASS=0
FAIL=0
TOTAL=0

assert_exit_code() {
    local desc="$1" expected="$2" actual="$3"
    TOTAL=$((TOTAL + 1))
    echo ""
    echo "--- $desc ---"
    if [[ "$actual" -eq "$expected" ]]; then
        echo "  ✅ PASS: exit code $actual (expected $expected)"
        PASS=$((PASS + 1))
    else
        echo "  ❌ FAIL: exit code $actual (expected $expected)"
        FAIL=$((FAIL + 1))
	exit 1
    fi
}

assert_flag() {
    local desc="$1" node="$2" expected="$3"
    TOTAL=$((TOTAL + 1))
    echo ""
    echo "--- $desc ---"
    local val
    val=$(zfs get -H -o value zep:error:split-brain zep-node-${node}/test-${node} 2>/dev/null)
    if [[ "$val" == "$expected" ]]; then
        echo "  ✅ PASS: node${node} split-brain flag = '$val'"
        PASS=$((PASS + 1))
    else
        echo "  ⚠️  node${node} split-brain flag = '$val' (expected '$expected')"
    fi
}

rollback_node() {
    local node="$1"
    local snap
    snap=$(zfs list -t snap -o name -H -S creation zep-node-${node}/test-${node} 2>/dev/null | grep '@zep_min1-' | head -1 | cut -d@ -f2)
    if [[ -z "$snap" ]]; then
        echo "  ⚠️  No min1 snapshot found on node${node}"
        return 1
    fi
    echo "  Rolling back node${node} to @${snap}..."
    zfs rollback -r zep-node-${node}/test-${node}@${snap} 2>/dev/null
    return 0
}

write_error() {
    local node="$1"
    rm -f /zep-node-${node}/test-${node}/error 2>/dev/null || true
    echo "divergent" > /zep-node-${node}/test-${node}/error
}

clear_error() {
    local node="$1"
    rm -f /zep-node-${node}/test-${node}/error 2>/dev/null || true
}

ensure_chain() {
    for n in "${NODES[@]}"; do
        $ZEP --alias node${n} zep-node-${n}/test-${n} --config chain=node1,node2,node3 >/dev/null 2>&1
    done
}

# --- Phase 1: Healthy baseline ---
echo "=== Phase 1: Healthy baseline ==="
ensure_chain
rollback_node 2; rollback_node 3
echo "  Chain: node1 → node2 → node3"

rc=0
set +e
$ZEP --alias node1 zep-node-1/test-1 min1 >/tmp/zep.log 2>&1
rc=$?
set -e
assert_exit_code "Healthy replication from node1" 0 "$rc"

# --- Phase 2: Split-brain on node2 → exit 2 ---
echo ""
echo "=== Phase 2: Split-brain on node2 ==="

write_error 2
echo "  Divergent data written to node2"

echo sleep 1 minute
sleep 60

rc=0
set +e
$ZEP --alias node1 zep-node-1/test-1 min1 >/tmp/zep.log 2>&1
rc=$?
set -e

assert_exit_code "Replication from node1 with split-brain on node2" 2 "$rc"
assert_flag "Split-brain flag set on node2" 2 "true"

# --- Phase 3: Resilience mode → exit 0 ---
echo ""
echo "=== Phase 3: Resilience mode ==="

zfs set zep:policy=resilience zep-node-1/test-1 2>/dev/null
echo "  policy=resilience set on node1 (master)"

echo sleep 1 minute
sleep 60

rc=0
set +e
$ZEP --alias node1 zep-node-1/test-1 min1 -bw >/tmp/zep.log 2>&1
rc=$?
set -e

assert_exit_code "Replication with resilience, split-brain on node2" 0 "$rc"

# --- Phase 4: Rollback node2, verify recovery ---
echo ""
echo "=== Phase 4: Rollback node2 and recovery ==="

zfs set zep:policy=fail zep-node-1/test-1 2>/dev/null
rollback_node 2

assert_flag "Split-brain flag after rollback on node2" 2 "true"


echo sleep 1 minute
sleep 60

rc=0
set +e
$ZEP --alias node1 zep-node-1/test-1 min1 -bw >/tmp/zep.log 2>&1
rc=$?
set -e

assert_exit_code "Replication after node2 rollback" 0 "$rc"
exit 0

assert_flag "Split-brain flag cleared by replication" 2 "false"

# --- Phase 5: Split-brain on node3 (deep in chain) → exit 2 ---
echo ""
echo "=== Phase 5: Split-brain on node3 ==="

write_error 3
echo "  Divergent data written to node3"

echo sleep 1 minute
sleep 60

rc=0
set +e
$ZEP --alias node1 zep-node-1/test-1 min1 -bw >/tmp/zep.log 2>&1
rc=$?
set -e

assert_exit_code "Replication from node1 with split-brain on node3" 2 "$rc"
assert_flag "Split-brain flag set on node3" 3 "true"

# --- Phase 6: Resilience mode with node3 divergence → exit 0 ---
echo ""
echo "=== Phase 6: Resilience mode with node3 ==="

zfs set zep:policy=resilience zep-node-1/test-1 2>/dev/null


echo sleep 1 minute
sleep 60

rc=0
set +e
$ZEP --alias node1 zep-node-1/test-1 min1 -bw >/tmp/zep.log 2>&1
rc=$?
set -e

assert_exit_code "Replication with resilience, split-brain on node3" 0 "$rc"

# --- Phase 7: Rollback node3, verify recovery ---
echo ""
echo "=== Phase 7: Rollback node3 and recovery ==="

zfs set zep:policy=fail zep-node-1/test-1 2>/dev/null
rollback_node 3

assert_flag "Split-brain flag after rollback on node3" 3 "true"

echo sleep 1 minute
sleep 60

rc=0
set +e
$ZEP --alias node1 zep-node-1/test-1 min1 -bw >/tmp/zep.log 2>&1
rc=$?
set -e

assert_exit_code "Replication after node3 rollback" 0 "$rc"
assert_flag "Split-brain flag cleared by replication" 3 "false"

# --- Summary ---
echo ""
echo "========================================"
echo "  Results: $PASS/$TOTAL passed, $FAIL failed"
echo "========================================"
[[ $FAIL -eq 0 ]] && exit 0 || exit 1
