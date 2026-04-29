#!/bin/bash

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
CONF_FILE="$SCRIPT_DIR/test.conf"
[[ -f "$CONF_FILE" ]] && source "$CONF_FILE"

ZEP_BIN=$(command -v zep)
DS="zep-node-1/test-1"
LABEL="min1"

PASS=0
FAIL=0

RED='\e[31m'
GREEN='\e[32m'
YELLOW='\e[33m'
CYAN='\e[36m'
RESET='\e[0m'

# ── test filtering ───────────────────────────────────────

RUN_TESTS=()
SKIP_TESTS=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --test)
            while [[ $# -gt 1 && ! "$2" =~ ^-- ]]; do
                RUN_TESTS+=("$2"); shift
            done
            shift ;;
        --skip)
            while [[ $# -gt 1 && ! "$2" =~ ^-- ]]; do
                SKIP_TESTS+=("$2"); shift
            done
            shift ;;
        *) shift ;;
    esac
done

should_run() {
    local t="$1"
    if [[ ${#RUN_TESTS[@]} -gt 0 ]]; then
        for rt in "${RUN_TESTS[@]}"; do [[ "$rt" == "$t" ]] && return 0; done
        return 1
    fi
    for st in "${SKIP_TESTS[@]}"; do [[ "$st" == "$t" ]] && return 1; done
    return 0
}

# ── helpers ──────────────────────────────────────────────

clean_tmp() { rm -rf /tmp/zep_* 2>/dev/null || true; }

assert_exit() {
    local name="$1" expected="$2" actual="$3"
    if [[ "$expected" == "0" && "$actual" -eq 0 ]] || [[ "$expected" == "!0" && "$actual" -ne 0 ]] || [[ "$expected" =~ ^[0-9]+$ && "$actual" -eq "$expected" ]]; then
        echo -e "  ${GREEN}PASS${RESET} $name"
        ((PASS++))
    else
        echo -e "  ${RED}FAIL${RESET} $name (expected ${expected}, got ${actual})"
        ((FAIL++))
    fi
}

assert_out() {
    local name="$1" output="$2" pattern="$3" expect="${4:-yes}"
    if echo "$output" | grep -qF "$pattern"; then
        if [[ "$expect" == "yes" ]]; then
            echo -e "  ${GREEN}PASS${RESET} $name"; ((PASS++))
        else
            echo -e "  ${RED}FAIL${RESET} $name (unexpected match: $pattern)"; ((FAIL++))
        fi
    else
        if [[ "$expect" == "yes" ]]; then
            echo -e "  ${RED}FAIL${RESET} $name (missing: $pattern)"; ((FAIL++))
        else
            echo -e "  ${GREEN}PASS${RESET} $name"; ((PASS++))
        fi
    fi
}

run_zep() {
    clean_tmp
    local out rc
    out=$("$ZEP_BIN" "$@" 2>&1)
    rc=$?
    echo "$out"
    return $rc
}

destroy_node3() { zfs destroy -r zep-node-3/test-3 2>/dev/null || true; }

# ── init ─────────────────────────────────────────────────

echo -e "${CYAN}=== Zeplicator Replication Test Suite ===${RESET}\n"

echo -e "${YELLOW}[setup]${RESET} Initializing..."
# Clean up any leftover pools from previous aborted runs
for i in 1 2 3; do
    zpool import -f "zep-node-$i" 2>/dev/null && zpool destroy -f "zep-node-$i" 2>/dev/null || true
    zpool labelclear -f "/tmp/zep-ramdisk/zep-node-$i.img" 2>/dev/null || true
done
bash "$SCRIPT_DIR/init.sh" > /dev/null 2>&1 || { echo "  init.sh failed"; exit 1; }
clean_tmp
echo -e "  ${GREEN}OK${RESET}"

if should_run 1; then
# ══════════════════════════════════════════════════════════
# TEST 1: INIT_CLEAN — initial replication, clean dest
# ══════════════════════════════════════════════════════════
echo -e "\n${CYAN}[1] INIT_CLEAN${RESET}"
destroy_node3
out=$(run_zep "$DS" --alias node1 "$LABEL" --init); rc=$?
assert_exit "exit 0"   "0" "$rc"
assert_out  "cascade"  "$out" "VERIFICATION SUCCESS"
assert_out  "shipped"  "$out" "Marking sent snapshot"
for i in 1 2 3; do
    cnt=$(zfs list -t snap -H -o name -r zep-node-$i/test-$i 2>/dev/null | grep -c "$LABEL" || echo 0)
    assert_out "node$i snaps" "$cnt" "1"
done
fi

if should_run 2; then
# ══════════════════════════════════════════════════════════
# TEST 2: INCREMENTAL — normal incremental run
# ══════════════════════════════════════════════════════════
echo -e "\n${CYAN}[2] INCREMENTAL${RESET}"
out=$(run_zep "$DS" --alias node1 "$LABEL"); rc=$?
assert_exit "exit 0"   "0" "$rc"
assert_out  "cascade"  "$out" "VERIFICATION SUCCESS"
fi

if should_run 3; then
# ══════════════════════════════════════════════════════════
# TEST 3: FOREIGN_DATASET — node3 has snaps, no common ground
# ══════════════════════════════════════════════════════════
echo -e "\n${CYAN}[3] FOREIGN_DATASET${RESET}"
destroy_node3
zfs create zep-node-3/test-3
zfs snapshot zep-node-3/test-3@alien_snap
zfs set canmount=noauto zep-node-3/test-3
zfs unmount zep-node-3/test-3 2>/dev/null || true
zfs allow zep-user-3 create,mount,receive,destroy,userprop,diff zep-node-3 2>/dev/null
out=$(run_zep "$DS" --alias node1 "$LABEL" --init); rc=$?
assert_exit "exit !0"  "!0" "$rc"
assert_out  "FOREIGN"  "$out" "FOREIGN DATASET"
# cleanup: reset node3 for subsequent tests
destroy_node3
zfs create -o canmount=noauto zep-node-3/test-3
zfs unmount zep-node-3/test-3 2>/dev/null || true
zfs allow zep-user-3 create,destroy,send,receive,snapshot,hold,release,userprop zep-node-3/test-3 2>/dev/null || true
run_zep "$DS" --alias node1 "$LABEL" --init > /dev/null
fi

if should_run 4; then
# ══════════════════════════════════════════════════════════
# TEST 4: MISSING_PERMS — revoke pool perms on node3
# ══════════════════════════════════════════════════════════
echo -e "\n${CYAN}[4] MISSING_PERMS${RESET}"
destroy_node3
# re-init so node2+node3 are healthy
out=$(run_zep "$DS" --alias node1 "$LABEL" --init > /dev/null)
zfs unallow zep-user-3 zep-node-3 2>/dev/null || true
out=$(run_zep "$DS" --alias node1 "$LABEL"); rc=$?
assert_exit "exit !0"  "!0" "$rc"
assert_out  "perms msg" "$out" "Missing pool permissions"
# restore
zfs allow zep-user-3 create,mount,receive,destroy,userprop,diff zep-node-3 2>/dev/null
zfs allow zep-user-3 create,destroy,send,receive,snapshot,hold,release,userprop,diff zep-node-3/test-3 2>/dev/null || true
fi

if should_run 5; then
# ══════════════════════════════════════════════════════════
# TEST 5: DIVERGENCE — node3 has data written since snapshot
# ══════════════════════════════════════════════════════════
echo -e "\n${CYAN}[5] DIVERGENCE${RESET}"
destroy_node3
run_zep "$DS" --alias node1 "$LABEL" --init > /dev/null
# Write 256K to trigger referenced change
zfs set canmount=on zep-node-3/test-3; zfs mount zep-node-3/test-3 2>/dev/null
dd if=/dev/urandom of=/zep-node-3/test-3/div.bin bs=64K count=4 conv=fsync 2>/dev/null
sync; sleep 1
zfs unmount zep-node-3/test-3 2>/dev/null; zfs set canmount=noauto zep-node-3/test-3

out=$(run_zep "$DS" --alias node1 "$LABEL"); rc=$?
assert_exit "exit !0"  "!0" "$rc"
assert_out  "DIVERGENCE" "$out" "DIVERGENCE DETECTED"
fi

if should_run 6; then
# ══════════════════════════════════════════════════════════
# TEST 6: DIVERGENCE_OVERRIDE — -y forces through
# ══════════════════════════════════════════════════════════
echo -e "\n${CYAN}[6] DIVERGENCE_OVERRIDE${RESET}"
out=$(run_zep "$DS" --alias node1 "$LABEL" -y); rc=$?
assert_exit "exit 0"   "0" "$rc"
assert_out  "forcing"  "$out" "Forcing alignment"
fi

if should_run 7; then
# ══════════════════════════════════════════════════════════
# TEST 7: NON_MASTER_SKIP — node2 skips snapshot creation
# ══════════════════════════════════════════════════════════
echo -e "\n${CYAN}[7] NON_MASTER_SKIP${RESET}"
out=$(run_zep "zep-node-2/test-2" --alias node2 "$LABEL"); rc=$?
assert_exit "exit !0"  "!0" "$rc"
assert_out  "not master" "$out" "not Master"
fi

if should_run 8; then
# ══════════════════════════════════════════════════════════
# TEST 8: MISSING_POOL — target pool exported
# ══════════════════════════════════════════════════════════
echo -e "\n${CYAN}[8] MISSING_POOL${RESET}"
destroy_node3; run_zep "$DS" --alias node1 "$LABEL" --init > /dev/null
zfs unmount zep-node-3/test-3 2>/dev/null || true
zpool export zep-node-3
out=$(run_zep "$DS" --alias node1 "$LABEL"); rc=$?
zpool import -f -d /tmp/zep-ramdisk zep-node-3 2>/dev/null || true
assert_exit "exit !0"  "!0" "$rc"
assert_out  "pool msg" "$out" "not found"
fi

if should_run 9; then
# ══════════════════════════════════════════════════════════
# TEST 9: STATUS — status command works
# ══════════════════════════════════════════════════════════
echo -e "\n${CYAN}[9] STATUS${RESET}"
destroy_node3; run_zep "$DS" --alias node1 "$LABEL" --init > /dev/null
out=$(run_zep "$DS" --alias node1 --status --force-color); rc=$?
assert_exit "exit 0"   "0" "$rc"
assert_out  "node1"    "$out" "node1"
assert_out  "node2"    "$out" "node2"
assert_out  "node3"    "$out" "node3"
fi

if should_run 10; then
# ══════════════════════════════════════════════════════════
# TEST 10: ROTATE — retention keeps count within limit
# ══════════════════════════════════════════════════════════
echo -e "\n${CYAN}[10] ROTATE${RESET}"
for i in 1 2 3; do clean_tmp; run_zep "$DS" --alias node1 "$LABEL" > /dev/null; done
run_zep "$DS" --alias node1 --rotate > /dev/null
cnt=$(zfs list -t snap -H -o name -r "$DS" 2>/dev/null | grep -c "$LABEL" || echo 0)
if [[ $cnt -le 10 ]]; then
    echo -e "  ${GREEN}PASS${RESET} rotate count $cnt <= 10"; ((PASS++))
else
    echo -e "  ${RED}FAIL${RESET} rotate count $cnt > 10"; ((FAIL++))
fi
fi

# ══════════════════════════════════════════════════════════
# RESUME TESTS — throttle + timeout interrupt
# ══════════════════════════════════════════════════════════

echo -e "\n${CYAN}=== Resume / Recovery Tests ===${RESET}"

setup_resume_mode() {
    zfs set zep:debug:throttle=64k "$DS"
    zfs set zep:debug:send_timeout=10 "$DS"
    zep "$DS" --alias node1 --config zep:zfs:recv_opt="-F -s" --all > /dev/null 2>&1
}
teardown_resume_mode() {
    zfs inherit zep:debug:throttle "$DS" 2>/dev/null || zfs set zep:debug:throttle=- "$DS"
    zfs inherit zep:debug:send_timeout "$DS" 2>/dev/null || zfs set zep:debug:send_timeout=0 "$DS"
    zep "$DS" --alias node1 --config zep:debug:throttle=- --all > /dev/null 2>&1
    zep "$DS" --alias node1 --config zep:debug:send_timeout=0 --all > /dev/null 2>&1
}

if should_run 11; then
# ══════════════════════════════════════════════════════════
# TEST 11: RESUME — interrupted transfer resumes and completes
# ══════════════════════════════════════════════════════════
echo -e "\n${CYAN}[11] RESUME — interrupted transfer resumes, completes within retries${RESET}"
destroy_node3
run_zep "$DS" --alias node1 "$LABEL" --init > /dev/null

setup_resume_mode

# Write enough data to trigger a transfer that may resume (~2MB)
zfs set canmount=on zep-node-1/test-1; zfs mount zep-node-1/test-1 2>/dev/null
dd if=/dev/urandom of=/zep-node-1/test-1/resume_big.dat bs=1M count=2 conv=fsync 2>/dev/null
sync

# Run — expect interruption
out=$(run_zep "$DS" --alias node1 "$LABEL"); rc=$?
assert_exit "interrupted" "!0" "$rc"
assert_out  "timeout msg" "$out" "iomon: timeout"

# Retry until complete (resume token persists across runs)
# Each run at 64k/s 10s timeout transfers ~640KB, 2MB needs ~4 retries
completed=false
for attempt in $(seq 1 30); do
    clean_tmp
    out=$(run_zep "$DS" --alias node1 "$LABEL" --init); rc=$?
    if [[ $rc -eq 0 ]]; then
        completed=true
        break
    fi
done
if [[ "$completed" == true ]]; then
    echo -e "  ${GREEN}PASS${RESET} completed within 30 retries"; ((PASS++))
else
    echo -e "  ${RED}FAIL${RESET} did not complete in 30 retries"; ((FAIL++))
fi

teardown_resume_mode
fi

if should_run 12; then
# ══════════════════════════════════════════════════════════
# TEST 12: RESUME_FAILED — snapshots destroyed mid-transfer
# ══════════════════════════════════════════════════════════
echo -e "\n${CYAN}[12] RESUME_FAILED — mid-transfer snapshot loss, recovers${RESET}"
destroy_node3
run_zep "$DS" --alias node1 "$LABEL" --init > /dev/null

setup_resume_mode

# Create 5 snapshots with ~2MB data each
zfs mount zep-node-1/test-1 2>/dev/null
for i in $(seq 1 5); do
    dd if=/dev/urandom of=/zep-node-1/test-1/snap_${i}.dat bs=1M count=2 conv=fsync 2>/dev/null
    sync
    zfs snapshot "zep-node-1/test-1@zep_${LABEL}_snap_${i}" 2>/dev/null
done

# Run — will be interrupted, token saved on node2
out=$(run_zep "$DS" --alias node1 "$LABEL" --init); rc=$?
assert_exit "interrupted" "!0" "$rc"
assert_out  "timeout msg" "$out" "iomon: timeout"

# Verify resume token exists on node2
token=$(ssh zep-user-2@zep-node-2.local "zfs get -H -o value receive_resume_token zep-node-2/test-2" 2>/dev/null || echo "-")
if [[ "$token" != "-" ]]; then
    echo -e "  ${GREEN}PASS${RESET} resume token saved"; ((PASS++))
else
    echo -e "  ${RED}FAIL${RESET} resume token not saved"; ((FAIL++))
fi

# Destroy snapshots 2-4 on master (being transmitted)
for i in 2 3 4; do
    zfs destroy "zep-node-1/test-1@zep_${LABEL}_snap_${i}" 2>/dev/null || true
done

# Re-run — should detect ERR_RESUME_FAILED, clear token
out=$(run_zep "$DS" --alias node1 "$LABEL"); rc=$?
assert_exit "resume failed" "!0" "$rc"
assert_out  "token invalidated" "$out" "Resume token invalidated"
assert_out  "token destroyed" "$out" "Destroyed stale resume token"

# Token should be cleared
token=$(ssh zep-user-2@zep-node-2.local "zfs get -H -o value receive_resume_token zep-node-2/test-2" 2>/dev/null || echo "-")
if [[ "$token" == "-" ]]; then
    echo -e "  ${GREEN}PASS${RESET} token cleared after failure"; ((PASS++))
else
    echo -e "  ${RED}FAIL${RESET} token not cleared"; ((FAIL++))
fi

# Disable throttling, run clean — should complete with --init for node3
teardown_resume_mode
out=$(run_zep "$DS" --alias node1 "$LABEL" --init); rc=$?
assert_exit "clean run after failure" "0" "$rc"

# Remaining snaps (1 and 5) should reach node2
rem=$(ssh zep-user-2@zep-node-2.local "zfs list -t snap -H -o name zep-node-2/test-2 2>/dev/null | grep -c 'zep_${LABEL}_snap_'" 2>/dev/null || echo 0)
rem=$(echo "$rem" | tr -d '[:space:]')
if [[ -n "$rem" && "$rem" -ge 2 ]]; then
    echo -e "  ${GREEN}PASS${RESET} remaining snaps on sink: $rem >= 2"; ((PASS++))
else
    echo -e "  ${RED}FAIL${RESET} remaining snaps on sink: '$rem' < 2"; ((FAIL++))
fi
fi

# ══════════════════════════════════════════════════════════
# RESILIENCE TESTS — policy=resilience tolerates offline nodes
# ══════════════════════════════════════════════════════════

echo -e "\n${CYAN}=== Resilience Tests ===${RESET}"

isolate_node() {
    local node="$1"
    sed -i "/zep-node-${node}.local/d" /etc/hosts
}

restore_node() {
    local node="$1"
    if ! grep -q "zep-node-${node}.local" /etc/hosts 2>/dev/null; then
        echo "127.0.0.1 zep-node-${node}.local" >> /etc/hosts
    fi
}

if should_run 13; then
# ══════════════════════════════════════════════════════════
# TEST 13: RESILIENCE_NODE2_OFFLINE — node2 unreachable, exits 3
# ══════════════════════════════════════════════════════════
echo -e "\n${CYAN}[13] RESILIENCE_NODE2_OFFLINE — node2 offline, exits 3${RESET}"
destroy_node3
run_zep "$DS" --alias node1 "$LABEL" --init > /dev/null

# Set policy=resilience on master
"$ZEP_BIN" "$DS" --alias node1 --config policy=resilience > /dev/null

# Isolate node2 (last in chain: node1→node3→node2, partial success expected)
isolate_node 2

for cycle in 1 2 3; do
    out=$(run_zep "$DS" --alias node1 "$LABEL"); rc=$?
    assert_exit "cycle $cycle exit 3" "3" "$rc"
    assert_out  "skip node2" "$out" "WARNING: Downstream cascade from node3 failed"
done

# Verify node3 still receives snapshots even though node2 is down
snap_cnt=$(zfs list -t snap -H -o name -r zep-node-3/test-3 2>/dev/null | grep -c "$LABEL" || echo 0)
assert_out "node3 got snaps while node2 offline" "$snap_cnt" "4" # 1 from --init + 3 cycles
fi

if should_run 14; then
# ══════════════════════════════════════════════════════════
# TEST 14: RESILIENCE_NODE2_RECOVERY — restore node2, full success
# ══════════════════════════════════════════════════════════
echo -e "\n${CYAN}[14] RESILIENCE_NODE2_RECOVERY — restore node2, exits 0${RESET}"

restore_node 2
sleep 1 # let SSH cache clear

out=$(run_zep "$DS" --alias node1 "$LABEL"); rc=$?
assert_exit "restored exit 0" "0" "$rc"
assert_out  "cascade ok" "$out" "VERIFICATION SUCCESS"

# Reset policy to fail for subsequent test runs
"$ZEP_BIN" "$DS" --alias node1 --config policy=fail > /dev/null
fi

# ══════════════════════════════════════════════════════════
# PROMOTION TESTS — chain master rotation
# ══════════════════════════════════════════════════════════

echo -e "\n${CYAN}=== Promotion Tests ===${RESET}"

_get_chain() {
    local node="$1"
    zfs get -H -o value zep:chain "zep-node-${node}/test-${node}" 2>/dev/null
}

_promote() {
    local node="$1"
    local rc
    "$ZEP_BIN" --alias "node${node}" "zep-node-${node}/test-${node}" --promote --auto -y > /dev/null 2>&1
    rc=$?
    return $rc
}

if should_run 15; then
# ══════════════════════════════════════════════════════════
# TEST 15: PROMOTE_NODE3 — promote middle node to master
# ══════════════════════════════════════════════════════════
echo -e "\n${CYAN}[15] PROMOTE_NODE3 — promote node3 to master${RESET}"
destroy_node3
run_zep "$DS" --alias node1 "$LABEL" --init > /dev/null

_promote 3; rc=$?
assert_exit "promote node3" "0" "$rc"

chain=$(_get_chain 1)
assert_out "chain node3,node1,node2" "$chain" "node3,node1,node2"

out=$(run_zep "zep-node-3/test-3" --alias node3 "$LABEL"); rc=$?
assert_exit "node3 master exit 0" "0" "$rc"
assert_out  "node3 shipped" "$out" "Marking sent snapshot"

out=$(run_zep "zep-node-1/test-1" --alias node1 "$LABEL"); rc=$?
assert_exit "node1 non-master exit !0" "!0" "$rc"
fi

if should_run 16; then
# ══════════════════════════════════════════════════════════
# TEST 16: PROMOTE_NODE1_BACK — restore original master
# ══════════════════════════════════════════════════════════
echo -e "\n${CYAN}[16] PROMOTE_NODE1_BACK — promote node1 back to master${RESET}"

_promote 1; rc=$?
assert_exit "promote node1" "0" "$rc"

chain=$(_get_chain 1)
assert_out "chain restored" "$chain" "node1,node3,node2"

out=$(run_zep "$DS" --alias node1 "$LABEL"); rc=$?
assert_exit "node1 master again exit 0" "0" "$rc"
assert_out  "node1 shipped" "$out" "Marking sent snapshot"

out=$(run_zep "zep-node-3/test-3" --alias node3 "$LABEL"); rc=$?
assert_exit "node3 non-master exit !0" "!0" "$rc"
fi

echo ""
echo -e "${CYAN}======================================${RESET}"
echo -e "${GREEN}PASS:${RESET} $PASS  ${RED}FAIL:${RESET} $FAIL"
if [[ $FAIL -eq 0 ]]; then
    echo -e "${GREEN}All tests passed.${RESET}"
else
    echo -e "${RED}$FAIL test(s) failed.${RESET}"
fi
echo -e "${CYAN}======================================${RESET}"

exit $FAIL
