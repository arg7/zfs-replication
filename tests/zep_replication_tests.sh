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
