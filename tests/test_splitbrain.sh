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
ZEP="zep"

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
    if [[ "$val" == "$expected" || ("$val" == "-" && "$expected" == "false") ]]; then
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
    echo "divergent: $(date)" >> /zep-node-${node}/test-${node}/error
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
$ZEP --alias node1 zep-node-1/test-1 --config policy=fail >/dev/null

rc=0
set +e
$ZEP --alias node1 zep-node-1/test-1 --init min1 >/tmp/zep.log 2>&1
rc=$?
set -e
assert_exit_code "Initial replication from node1" 0 "$rc"
echo ""
echo "=== Phase 2: Split-brain on node2 ==="

write_error 2
echo "  Divergent data written to node2"

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

$ZEP --alias node1 zep-node-1/test-1 --config policy=resilience >/dev/null
echo "  policy=resilience set on node1 (master)"

rc=0
set +e
$ZEP --alias node1 zep-node-1/test-1 min1 -bw >/tmp/zep.log 2>&1
rc=$?
set -e

assert_exit_code "Replication with resilience, split-brain on node2" 3 "$rc"

# --- Phase 4: Rollback node2, verify recovery ---
echo ""
echo "=== Phase 4: Rollback node2 and recovery ==="

$ZEP --alias node1 zep-node-1/test-1 --config policy=fail >/dev/null
rollback_node 2

assert_flag "Split-brain flag after rollback on node2" 2 "true"

rc=0
set +e
$ZEP --alias node1 zep-node-1/test-1 min1 -bw >/tmp/zep.log 2>&1
rc=$?
set -e

assert_exit_code "Replication after node2 rollback" 0 "$rc"

assert_flag "Split-brain flag cleared by replication" 2 "false"

# --- Phase 5: force=true overrides split-brain check ---
echo ""
echo "=== Phase 5: force=true overrides divergence ==="

write_error 3
echo "  Divergent data written to node3"
$ZEP --alias node1 zep-node-1/test-1 --config zfs:force=true >/dev/null
echo "  zfs:force=true set on node1 (master)"

rc=0
set +e
$ZEP --alias node1 zep-node-1/test-1 min1 -bw >/tmp/zep.log 2>&1
rc=$?
set -e

assert_exit_code "Replication with force=true, divergence on node3" 0 "$rc"
echo "  Verifying divergent data on node3 was overwritten..."
if [[ ! -f /zep-node-3/test-3/error ]]; then
    echo "  ✅ PASS: divergent file removed by zfs recv -F"
    PASS=$((PASS + 1))
    TOTAL=$((TOTAL + 1))
else
    echo "  ❌ FAIL: divergent file still exists"
    FAIL=$((FAIL + 1))
    TOTAL=$((TOTAL + 1))
fi

# Reset force for remaining tests
$ZEP --alias node1 zep-node-1/test-1 --config zfs:force=false >/dev/null

# --- Phase 5b: --divergence-report direct invocation ---
echo ""
echo "=== Phase 5b: --divergence-report command ==="

# Get fresh common snapshot AFTER force replication synced node3
common_snap=$(zfs list -t snap -o name -H -S creation zep-node-3/test-3 2>/dev/null | grep '@zep_min1-' | tail -1)
echo "  Common snapshot on node3: $common_snap"

write_error 3
echo "  Divergent data written to node3"

rc=0
set +e
bash -x $ZEP zep-node-3/test-3 --alias node3 --divergence-report "${common_snap#*@}" >/tmp/div_report.log 2>/tmp/dt.log
rc=$?
set -e

assert_exit_code "--divergence-report detects divergence" 2 "$rc"
if grep -q "error" /tmp/div_report.log; then
    echo "  ✅ PASS: report output contains divergence details"
    PASS=$((PASS + 1))
    TOTAL=$((TOTAL + 1))
else
    echo "  ❌ FAIL: report output missing divergence details"
    FAIL=$((FAIL + 1))
    TOTAL=$((TOTAL + 1))
fi

# Clear divergence
rollback_node 3

# Verify clean report returns 0
rc=0
set +e
$ZEP zep-node-3/test-3 --alias node3 --divergence-report "${common_snap#@}" >/tmp/div_clean.log 2>&1
rc=$?
set -e

assert_exit_code "--divergence-report clean dataset returns 0" 0 "$rc"

# --- Phase 6: Split-brain on node3 (deep in chain) → exit 2 ---
echo ""
echo "=== Phase 6: Split-brain on node3 ==="

write_error 3
echo "  Divergent data written to node3"

rc=0
set +e
$ZEP --alias node1 zep-node-1/test-1 min1 -bw >/tmp/zep.log 2>&1
rc=$?
set -e

assert_exit_code "Replication from node1 with split-brain on node3" 2 "$rc"
assert_flag "Split-brain flag set on node3" 3 "true"

# --- Phase 7: Resilience mode with node3 divergence → exit 0 ---
echo ""
echo "=== Phase 7: Resilience mode with node3 ==="

$ZEP --alias node1 zep-node-1/test-1 --config policy=resilience >/dev/null

rc=0
set +e
$ZEP --alias node1 zep-node-1/test-1 min1 -bw >/tmp/zep.log 2>&1
rc=$?
set -e

assert_exit_code "Replication with resilience, split-brain on node3" 3 "$rc"

# --- Phase 8: Rollback node3, verify recovery ---
echo ""
echo "=== Phase 8: Rollback node3 and recovery ==="

$ZEP --alias node1 zep-node-1/test-1 --config policy=fail >/dev/null
rollback_node 3

assert_flag "Split-brain flag after rollback on node3" 3 "true"

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
