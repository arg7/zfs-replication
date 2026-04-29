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

# ── helpers ──────────────────────────────────────────────

clean_tmp() { rm -rf /tmp/zep_* 2>/dev/null || true; }

assert_exit() {
    local name="$1" expected="$2" actual="$3"
    if [[ "$expected" == "0" && "$actual" -eq 0 ]] || [[ "$expected" == "!0" && "$actual" -ne 0 ]]; then
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

# ══════════════════════════════════════════════════════════
# TEST 2: INCREMENTAL — normal incremental run
# ══════════════════════════════════════════════════════════
echo -e "\n${CYAN}[2] INCREMENTAL${RESET}"
out=$(run_zep "$DS" --alias node1 "$LABEL"); rc=$?
assert_exit "exit 0"   "0" "$rc"
assert_out  "cascade"  "$out" "VERIFICATION SUCCESS"

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

# ══════════════════════════════════════════════════════════
# TEST 6: DIVERGENCE_OVERRIDE — -y forces through
# ══════════════════════════════════════════════════════════
echo -e "\n${CYAN}[6] DIVERGENCE_OVERRIDE${RESET}"
out=$(run_zep "$DS" --alias node1 "$LABEL" -y); rc=$?
assert_exit "exit 0"   "0" "$rc"
assert_out  "forcing"  "$out" "Forcing alignment"

# ══════════════════════════════════════════════════════════
# TEST 7: NON_MASTER_SKIP — node2 skips snapshot creation
# ══════════════════════════════════════════════════════════
echo -e "\n${CYAN}[7] NON_MASTER_SKIP${RESET}"
out=$(run_zep "zep-node-2/test-2" --alias node2 "$LABEL"); rc=$?
assert_exit "exit !0"  "!0" "$rc"
assert_out  "not master" "$out" "not Master"

# ══════════════════════════════════════════════════════════
# TEST 8: MISSING_POOL — target pool exported
# ══════════════════════════════════════════════════════════
echo -e "\n${CYAN}[8] MISSING_POOL${RESET}"
destroy_node3; run_zep "$DS" --alias node1 "$LABEL" --init > /dev/null
zfs unmount zep-node-3/test-3 2>/dev/null || true
zpool export zep-node-3
out=$(run_zep "$DS" --alias node1 "$LABEL"); rc=$?
zpool import -f zep-node-3 2>/dev/null || true
assert_exit "exit !0"  "!0" "$rc"
assert_out  "pool msg" "$out" "not found"

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

# ══════════════════════════════════════════════════════════
# RESUME TESTS — throttle + timeout interrupt
# ══════════════════════════════════════════════════════════

POOL_SIZE_BYTES=134217728   # 128M
BIG_FILE_SIZE=$((POOL_SIZE_BYTES / 4))      # 32MB (~25%)
SMALL_FILE_SIZE=$((POOL_SIZE_BYTES * 3 / 100))  # ~4MB (~3%)

echo -e "\n${CYAN}=== Resume / Recovery Tests ===${RESET}"

# Helper: enable throttled+resumable mode, write data, then clean up
setup_resume_mode() {
    zfs set zep:throttle=16k "$DS"
    zfs set zep:debug:send_timeout=5 "$DS"
    zfs set zep:zfs:recv_opt="-F -s" "$DS"
}
teardown_resume_mode() {
    zfs inherit zep:throttle "$DS" 2>/dev/null || zfs set zep:throttle=- "$DS"
    zfs inherit zep:debug:send_timeout "$DS" 2>/dev/null || zfs set zep:debug:send_timeout=0 "$DS"
    zfs set zep:zfs:recv_opt="-F" "$DS"
}

write_file() {
    local path="$1" size="$2" name="$3"
    dd if=/dev/urandom of="$path/$name" bs=1M count=$((size / 1048576)) conv=fsync 2>/dev/null
}

# ══════════════════════════════════════════════════════════
# TEST 11: RESUME — interrupted transfer resumes and completes
# ══════════════════════════════════════════════════════════
echo -e "\n${CYAN}[11] RESUME — throttled + timeout, re-run completes${RESET}"
destroy_node3
run_zep "$DS" --alias node1 "$LABEL" --init > /dev/null

setup_resume_mode

# Write 32MB file to master
zfs set canmount=on zep-node-1/test-1; zfs mount zep-node-1/test-1 2>/dev/null
write_file /zep-node-1/test-1 "$BIG_FILE_SIZE" "resume_big.dat"
sync

# Run with throttle+timeout — should be interrupted
out=$(run_zep "$DS" --alias node1 "$LABEL"); rc=$?
assert_out  "interrupted" "$out" "Pipeline failed" "yes"

# Run again — might still be interrupted (transfer is big, 5s may not be enough per run)
attempts=1
while [[ $attempts -le 5 ]]; do
    clean_tmp
    out=$(run_zep "$DS" --alias node1 "$LABEL"); rc=$?
    if [[ $rc -eq 0 ]]; then
        break
    fi
    ((attempts++))
done
assert_exit "completed within 5 retries" "0" "$rc"

# Verify big file reached node3
cnt=$(ssh zep-user-3@zep-node-3.local "ls /zep-node-3/test-3/resume_big.dat 2>/dev/null | wc -l" 2>/dev/null || echo 0)
assert_out  "file on sink" "$cnt" "1"

teardown_resume_mode

# ══════════════════════════════════════════════════════════
# TEST 12: RESUME_FAILED — snapshots destroyed mid-transfer
# ══════════════════════════════════════════════════════════
echo -e "\n${CYAN}[12] RESUME_FAILED — mid-transfer snapshot loss, recovers${RESET}"
destroy_node3
run_zep "$DS" --alias node1 "$LABEL" --init > /dev/null

setup_resume_mode

# Create 10 snapshots on master, each with ~4MB of data
zfs mount zep-node-1/test-1 2>/dev/null
for i in $(seq 1 10); do
    write_file /zep-node-1/test-1 "$SMALL_FILE_SIZE" "snap_${i}_data.dat"
    sync
    zfs snapshot "zep-node-1/test-1@zep_${LABEL}_snap_${i}" 2>/dev/null
done

# Run replication — will be interrupted by iomon timeout mid-way
out=$(run_zep "$DS" --alias node1 "$LABEL"); rc=$?
assert_out  "interrupted" "$out" "Pipeline failed" "yes"

# Check that a resume token exists on node3
token=$(ssh zep-user-3@zep-node-3.local "zfs get -H -o value receive_resume_token zep-node-3/test-3" 2>/dev/null || echo "-")
assert_out  "resume token saved on sink" "$token" "-" "no"  # should NOT be "-"

# Destroy some of the source snapshots being transmitted (snaps 3-7)
for i in 3 4 5 6 7; do
    zfs destroy "zep-node-1/test-1@zep_${LABEL}_snap_${i}" 2>/dev/null || true
done

# Re-run — zfs send should detect ERR_RESUME_FAILED, clear token, alert, exit !0
out=$(run_zep "$DS" --alias node1 "$LABEL"); rc=$?
assert_exit "resume failed detected" "!0" "$rc"
assert_out  "ERR_RESUME_FAILED in output" "$out" "cannot resume" "yes"

# Disable throttling/timeout, re-run — should complete cleanly
teardown_resume_mode
out=$(run_zep "$DS" --alias node1 "$LABEL"); rc=$?
assert_exit "clean run after resume failure" "0" "$rc"

# Verify remaining snapshots (1,2,8,9,10) reached node3
rem_snaps=$(ssh zep-user-3@zep-node-3.local "zfs list -t snap -H -o name zep-node-3/test-3 2>/dev/null | grep 'zep_${LABEL}_snap_' | wc -l" 2>/dev/null || echo 0)
if [[ $rem_snaps -ge 5 ]]; then
    echo -e "  ${GREEN}PASS${RESET} remaining snaps on sink: $rem_snaps >= 5"
    ((PASS++))
else
    echo -e "  ${RED}FAIL${RESET} remaining snaps on sink: $rem_snaps < 5"
    ((FAIL++))
fi

teardown_resume_mode

# ── summary ──────────────────────────────────────────────

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
