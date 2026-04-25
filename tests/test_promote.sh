#!/bin/bash
set -euo pipefail

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
[[ -f "$SCRIPT_DIR/test.conf" ]] && source "$SCRIPT_DIR/test.conf"

NUM_NODES=${NUM_NODES:-3}
ZEP="$SCRIPT_DIR/../build/zep"
INITIAL_CHAIN="${INITIAL_CHAIN:-node1,node2,node3}"

NODES=()
for (( i=1; i<=NUM_NODES; i++ )); do NODES+=("$i"); done

# Build default initial chain if not set
if [[ "$INITIAL_CHAIN" == "node1,node2,node3" && $NUM_NODES -ne 3 ]]; then
    INITIAL_CHAIN=""
    for (( i=1; i<=NUM_NODES; i++ )); do
        [[ -n "$INITIAL_CHAIN" ]] && INITIAL_CHAIN+=","
        INITIAL_CHAIN+="node${i}"
    done
fi
PASS=0
FAIL=0
TOTAL=0

_reset_chain() {
    local node="$1" chain="$2"
    ssh root@zep-node-${node}.local "zfs set zep:chain=${chain} zep-node-${node}/test-${node}" 2>/dev/null
}

_get_chain() {
    local node="$1"
    ssh root@zep-node-${node}.local "zfs get -H -o value zep:chain zep-node-${node}/test-${node}" 2>/dev/null
}

_check_all_chains() {
    local expected="$1"
    local n bad=0
    for n in "${NODES[@]}"; do
        local got
        got=$(_get_chain "$n")
        if [[ "$got" != "$expected" ]]; then
            echo "  FAIL: node${n}: expected '$expected', got '$got'"
            bad=$((bad + 1))
        else
            echo "  OK:   node${n}: '$got'"
        fi
    done
    return $bad
}

assert_chain_propagated() {
    local desc="$1" expected="$2"
    TOTAL=$((TOTAL + 1))
    echo ""
    echo "--- $desc ---"
    if _check_all_chains "$expected"; then
        echo "  ✅ PASS: $desc"
        PASS=$((PASS + 1))
    else
        echo "  ❌ FAIL: $desc"
        FAIL=$((FAIL + 1))
    fi
}

assert_replication_exit_codes() {
    local desc="$1" master="$2"
    TOTAL=$((TOTAL + 1))
    echo ""
    echo "--- $desc ---"
    local n bad=0
    for n in "${NODES[@]}"; do
        local rc
        set +e
        $ZEP --alias node${n} zep-node-${n}/test-${n} min1 >/dev/null 2>&1
        rc=$?
        set -e
        if [[ "$n" -eq "$master" ]]; then
            if [[ $rc -eq 0 ]]; then
                echo "  OK:   node${n} (master): exit 0"
            else
                echo "  FAIL: node${n} (master): expected exit 0, got $rc"
                bad=$((bad + 1))
            fi
        else
            if [[ $rc -ne 0 ]]; then
                echo "  OK:   node${n}: exit $rc (non-master, expected non-zero)"
            else
                echo "  FAIL: node${n}: expected non-zero, got 0"
                bad=$((bad + 1))
            fi
        fi
    done
    if [[ $bad -eq 0 ]]; then
        echo "  ✅ PASS: $desc"
        PASS=$((PASS + 1))
    else
        echo "  ❌ FAIL: $desc"
        FAIL=$((FAIL + 1))
    fi
}

# --- Phase 1: Reset all nodes to initial chain ---
echo "=== Setting initial chain: $INITIAL_CHAIN ==="
for n in "${NODES[@]}"; do
    echo "  Configuring node${n}..."
    $ZEP --alias node${n} zep-node-${n}/test-${n} --config chain=${INITIAL_CHAIN} 2>&1 | tail -1
done
echo ""
assert_chain_propagated "Initial chain set on all nodes" "$INITIAL_CHAIN"

# --- Phase 2-N: Rotate promotion through all nodes ---
# Start from node2, go through all nodes, end back at node1
PROMOTE_ORDER=()
for (( i=2; i<=NUM_NODES; i++ )); do PROMOTE_ORDER+=("$i"); done
PROMOTE_ORDER+=("1")

prev_master=1
for promote_node in "${PROMOTE_ORDER[@]}"; do
    echo ""
    echo "=== Promoting node${promote_node} to master ==="
    $ZEP --alias node${promote_node} zep-node-${promote_node}/test-${promote_node} --promote --auto -y 2>&1

    # Calculate expected chain after this promotion
    # Promotion moves the promoted node to position 0, rest keep relative order
    IFS=',' read -ra cur_nodes <<< "$INITIAL_CHAIN"
    # For each promotion, rotate: promoted node goes first, others follow in their current order
    # Simulate: take current chain, move promote_node to front
    new_chain="node${promote_node}"
    for cn in "${cur_nodes[@]}"; do
        [[ "$cn" != "node${promote_node}" ]] && new_chain+=",$cn"
    done

    assert_chain_propagated "After promoting node${promote_node}" "$new_chain"
    assert_replication_exit_codes "Replication run after node${promote_node} promotion" "$promote_node"

    # Update INITIAL_CHAIN for next iteration
    INITIAL_CHAIN="$new_chain"
    prev_master=$promote_node
done

# --- Summary ---
echo ""
echo "========================================"
echo "  Results: $PASS/$TOTAL passed, $FAIL failed"
echo "========================================"
[[ $FAIL -eq 0 ]] && exit 0 || exit 1
