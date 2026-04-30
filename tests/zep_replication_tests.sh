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
        --list)
            LIST_ONLY=true
            shift ;;
        --test)
            while [[ $# -gt 1 && ! "$2" =~ ^-- ]]; do
                printf -v padded '%02d' "$2" 2>/dev/null || padded="$2"
                RUN_TESTS+=("$padded"); shift
            done
            shift ;;
        --skip)
            while [[ $# -gt 1 && ! "$2" =~ ^-- ]]; do
                printf -v padded '%02d' "$2" 2>/dev/null || padded="$2"
                SKIP_TESTS+=("$padded"); shift
            done
            shift ;;
        [0-9]*)
            printf -v padded '%02d' "$1" 2>/dev/null || padded="$1"
            RUN_TESTS+=("$padded"); shift ;;
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

# ── test table ───────────────────────────────────────────

_test_table() {
    # format: NUM|DESCRIPTION|function_name
    cat <<'TABLE'
01|INIT_CLEAN|test_initial
02|INCREMENTAL|test_incremental
03|DIVERGENCE|test_divergence
04|DIVERGENCE_OVERRIDE|test_divergence_override
05|RESUME|test_resume
06|RESUME_FAILED|test_resume_failed
07|RESILIENCE NODE2 OFFLINE|test_resilience_offline
08|RESILIENCE NODE2 RECOVERY|test_resilience_recovery
09|SPLIT-BRAIN RESILIENCE|test_splitbrain_resilience
10|SPLIT-BRAIN ROLLBACK|test_splitbrain_rollback
11|DIVERGENCE REPORT|test_divergence_report
12|PROMOTE TO NODE3|test_promote
13|PROMOTE BACK TO NODE1|test_promote_back
14|NON-MASTER SKIP|test_non_master
15|STATUS|test_status
16|ROTATE|test_rotate
17|FOREIGN DATASET|test_foreign_dataset
18|MISSING PERMISSIONS|test_missing_perms
19|MISSING POOL|test_missing_pool
TABLE
}

list_tests() {
    echo -e "${CYAN}Available tests:${RESET}\n"
    while IFS='|' read -r num desc func; do
        printf "  ${GREEN}%2s${RESET}  %-26s (%s)\n" "$num" "$desc" "$func"
    done < <(_test_table)
    echo ""
}

if [[ "${LIST_ONLY:-false}" == "true" ]]; then
    list_tests
    exit 0
fi

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
    out=$("$ZEP_BIN" -bw "$@" </dev/null 2>&1)
    rc=$?
    echo "$out"
    echo "$out" >> "/tmp/test${TEST_NUM:-00}.log"
    return $rc
}

destroy_node3() { zfs destroy -r zep-node-3/test-3 2>/dev/null || true; }

# ── alert assertion helpers ───────────────────────────────

ALERTCON="${SCRIPT_DIR}/../build/alertcon"

_alert_count() {
    "$ALERTCON" --count 2>/dev/null || echo 0
}

_alert_since() {
    local since="$1"
    local now
    now=$(_alert_count)
    if [[ "$now" -gt "$since" ]]; then
        "$ALERTCON" --get "$((since+1))-LAST" --oneline 2>/dev/null
    fi
}

_assert_alert() {
    local desc="$1" output="$2" pattern="$3"
    if echo "$output" | grep -qi "$pattern"; then
        echo -e "  ${GREEN}PASS${RESET} alert: $desc"
        ((PASS++))
    else
        echo -e "  ${RED}FAIL${RESET} alert: $desc (missing: $pattern)"
        ((FAIL++))
    fi
}

_check_alerts() {
    local delta
    delta=$(_alert_since "$ALERT_BEFORE")
    if [[ -n "$delta" ]]; then
        {
            echo "--- alerts since #$ALERT_BEFORE ---"
            echo "$delta"
        } >> "/tmp/test${TEST_NUM:-00}.log"
    fi
    echo "$delta"
}

# ── section-specific helpers ─────────────────────────────

setup_resume_mode() {
    zfs set zep:debug:throttle=64k "$DS"
    zfs set zep:debug:send_timeout=10 "$DS"
    "$ZEP_BIN" -bw "$DS" --alias node1 --config zep:zfs:recv_opt="-F -s" --all </dev/null > /dev/null 2>&1
}
teardown_resume_mode() {
    zfs inherit zep:debug:throttle "$DS" 2>/dev/null || zfs set zep:debug:throttle=- "$DS"
    zfs inherit zep:debug:send_timeout "$DS" 2>/dev/null || zfs set zep:debug:send_timeout=0 "$DS"
    "$ZEP_BIN" -bw "$DS" --alias node1 --config zep:debug:throttle=- --all </dev/null > /dev/null 2>&1
    "$ZEP_BIN" -bw "$DS" --alias node1 --config zep:debug:send_timeout=0 --all </dev/null > /dev/null 2>&1
}

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

_get_chain() {
    local node="$1"
    zfs get -H -o value zep:chain "zep-node-${node}/test-${node}" 2>/dev/null
}

_promote() {
    local node="$1"
    local rc
    "$ZEP_BIN" -bw --alias "node${node}" "zep-node-${node}/test-${node}" --promote --auto -y </dev/null > /dev/null 2>&1
    rc=$?
    return $rc
}

# ── split-brain helpers ───────────────────────────────────

_write_error() {
    local node="$1"
    zfs set canmount=on "zep-node-${node}/test-${node}"
    zfs mount "zep-node-${node}/test-${node}" 2>/dev/null
    echo "divergent: $(date)" >> "/zep-node-${node}/test-${node}/error"
    sync
    zfs unmount "zep-node-${node}/test-${node}" 2>/dev/null
    zfs set canmount=noauto "zep-node-${node}/test-${node}"
}

_rollback_node() {
    local node="$1"
    local snap
    snap=$(zfs list -t snap -o name -H -S creation "zep-node-${node}/test-${node}" 2>/dev/null | grep '@zep_' | head -1 | cut -d@ -f2)
    if [[ -z "$snap" ]]; then
        echo "  ⚠️  No snapshot found on node${node}"
        return 1
    fi
    zfs rollback -r "zep-node-${node}/test-${node}@${snap}" 2>/dev/null
}

_check_flag() {
    local node="$1" expected="$2"
    local val
    val=$(zfs get -H -o value "zep:error:split-brain" "zep-node-${node}/test-${node}" 2>/dev/null)
    if [[ "$val" == "$expected" || ("$val" == "-" && "$expected" == "false") ]]; then
        echo -e "  ${GREEN}PASS${RESET} node${node} split-brain = '$val'"
        ((PASS++))
    else
        echo -e "  ${RED}FAIL${RESET} node${node} split-brain = '$val' (expected '$expected')"
        ((FAIL++))
    fi
}

_set_chain() {
    local chain="$1"
    for n in 1 2 3; do
        "$ZEP_BIN" -bw --alias "node${n}" "zep-node-${n}/test-${n}" --config "chain=${chain}" </dev/null > /dev/null 2>&1
    done
}

# ── init ─────────────────────────────────────────────────

echo -e "${CYAN}=== Zeplicator Replication Test Suite ===${RESET}\n"

chain_ok=false
if "$ZEP_BIN" -bw "zep-node-1/test-1" --status --alias node1 </dev/null > /dev/null 2>&1; then
    current_chain=$(_get_chain 1)
    if [[ "$current_chain" == "node1,node2,node3" ]]; then
        chain_ok=true
    fi
fi

if [[ "$chain_ok" == true ]]; then
    echo -e "${YELLOW}[setup]${RESET} Environment healthy, chain correct. Skipping init."
else
    echo -e "${YELLOW}[setup]${RESET} Initializing (chain: ${current_chain:-none} -> node1,node2,node3)..."
    for i in 1 2 3; do
        zpool import -f "zep-node-$i" 2>/dev/null && zpool destroy -f "zep-node-$i" 2>/dev/null || true
        zpool labelclear -f "/tmp/zep-ramdisk/zep-node-$i.img" 2>/dev/null || true
    done
    bash "$SCRIPT_DIR/init.sh" > /dev/null 2>&1 || { echo "  init.sh failed"; exit 1; }
    clean_tmp
fi
echo -e "  ${GREEN}OK${RESET}"

# ══════════════════════════════════════════════════════════
# TEST FUNCTIONS
# ══════════════════════════════════════════════════════════

test_initial() {
    destroy_node3
    out=$(run_zep "$DS" --alias node1 "$LABEL" --init); rc=$?
    assert_exit "exit 0"   "0" "$rc"
    assert_out  "cascade"  "$out" "VERIFICATION SUCCESS"
    assert_out  "shipped"  "$out" "Marking sent snapshot"
    for i in 1 2 3; do
        cnt=$(zfs list -t snap -H -o name -r zep-node-$i/test-$i 2>/dev/null | grep -c "$LABEL" || echo 0)
        assert_out "node$i snaps" "$cnt" "1"
    done
    local alerts; alerts=$(_check_alerts)
    _assert_alert "initial replication" "$alerts" "initial replication successful"
}

test_incremental() {
    out=$(run_zep "$DS" --alias node1 "$LABEL"); rc=$?
    assert_exit "exit 0"   "0" "$rc"
    assert_out  "cascade"  "$out" "VERIFICATION SUCCESS"
}

test_divergence() {
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
    local alerts; alerts=$(_check_alerts)
    _assert_alert "split-brain" "$alerts" "split-brain detected"
}

test_divergence_override() {
    out=$(run_zep "$DS" --alias node1 "$LABEL" -y); rc=$?
    assert_exit "exit 0"   "0" "$rc"
    assert_out  "forcing"  "$out" "Forcing alignment"
}

test_resume() {
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
}

test_resume_failed() {
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
    token=$(ssh -n zep-user-2@zep-node-2.local "zfs get -H -o value receive_resume_token zep-node-2/test-2" 2>/dev/null || echo "-")
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
    token=$(ssh -n zep-user-2@zep-node-2.local "zfs get -H -o value receive_resume_token zep-node-2/test-2" 2>/dev/null || echo "-")
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
    rem=$(ssh -n zep-user-2@zep-node-2.local "zfs list -t snap -H -o name zep-node-2/test-2 2>/dev/null | grep -c 'zep_${LABEL}_snap_'" 2>/dev/null || echo 0)
    rem=$(echo "$rem" | tr -d '[:space:]')
    if [[ -n "$rem" && "$rem" -ge 2 ]]; then
        echo -e "  ${GREEN}PASS${RESET} remaining snaps on sink: $rem >= 2"; ((PASS++))
    else
        echo -e "  ${RED}FAIL${RESET} remaining snaps on sink: '$rem' < 2"; ((FAIL++))
    fi
}

test_resilience_offline() {
    destroy_node3
    run_zep "$DS" --alias node1 "$LABEL" --init > /dev/null

    # Set policy=resilience on master
    "$ZEP_BIN" -bw "$DS" --alias node1 --config policy=resilience </dev/null > /dev/null

    # Isolate node2 (middle in chain: node1→node2→node3, skip + cascade to node3)
    isolate_node 2

    for cycle in 1 2 3; do
        out=$(run_zep "$DS" --alias node1 "$LABEL"); rc=$?
        assert_exit "cycle $cycle exit 3" "3" "$rc"
        assert_out  "skip node2" "$out" "WARNING: Downstream cascade from node3 failed"
    done

    # Verify node3 still receives snapshots even though node2 is down
    snap_cnt=$(zfs list -t snap -H -o name -r zep-node-3/test-3 2>/dev/null | grep -c "$LABEL" || echo 0)
    assert_out "node3 got snaps while node2 offline" "$snap_cnt" "4" # 1 from --init + 3 cycles
}

test_resilience_recovery() {
    restore_node 2
    sleep 1 # let SSH cache clear

    out=$(run_zep "$DS" --alias node1 "$LABEL"); rc=$?
    assert_exit "restored exit 0" "0" "$rc"
    assert_out  "cascade ok" "$out" "VERIFICATION SUCCESS"

    # Reset policy to fail for subsequent test runs
    "$ZEP_BIN" -bw "$DS" --alias node1 --config policy=fail </dev/null > /dev/null
}

test_splitbrain_resilience() {
    destroy_node3
    run_zep "$DS" --alias node1 "$LABEL" --init > /dev/null

    _write_error 2
    out=$(run_zep "$DS" --alias node1 "$LABEL"); rc=$?
    assert_exit "split-brain node2 exit 2" "2" "$rc"
    assert_out  "split-brain msg" "$out" "Split-Brain detected"
    _check_flag 2 "true"

    # Resilience: skip diverged node
    "$ZEP_BIN" -bw "$DS" --alias node1 --config policy=resilience </dev/null > /dev/null
    out=$(run_zep "$DS" --alias node1 "$LABEL"); rc=$?
    assert_exit "resilience skip exit 3" "3" "$rc"
    assert_out  "resilience skip" "$out" "Skipping due to policy=resilience"
    _check_flag 2 "true"
    local alerts; alerts=$(_check_alerts)
    _assert_alert "split-brain detected" "$alerts" "split-brain detected"
    _assert_alert "split-brain skipped"   "$alerts" "split-brain skipped"
}

test_splitbrain_rollback() {
    "$ZEP_BIN" -bw "$DS" --alias node1 --config policy=fail </dev/null > /dev/null

    _rollback_node 2
    _check_flag 2 "true"  # flag persists until successful replication

    out=$(run_zep "$DS" --alias node1 "$LABEL"); rc=$?
    assert_exit "rollback recovery exit 0" "0" "$rc"
    assert_out  "cascade ok" "$out" "VERIFICATION SUCCESS"
    _check_flag 2 "false"
}

test_divergence_report() {
    _write_error 2

    local snap
    snap=$(zfs list -t snap -o name -H -S creation zep-node-2/test-2 2>/dev/null | grep '@zep_' | head -1)

    out=$(run_zep "zep-node-2/test-2" --alias node2 --divergence-report "${snap#*@}"); rc=$?
    assert_exit "divergence-report exit 2" "2" "$rc"
    assert_out  "report has details" "$out" "Modified files"
    assert_out  "detects error" "$out" "error"

    _rollback_node 2

    out=$(run_zep "zep-node-2/test-2" --alias node2 --divergence-report "${snap#*@}"); rc=$?
    assert_exit "divergence-report clean exit 0" "0" "$rc"

    # Cleanup: reset node3 for subsequent tests
    destroy_node3
    run_zep "$DS" --alias node1 "$LABEL" --init > /dev/null
}

test_promote() {
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
}

test_promote_back() {
    _promote 1; rc=$?
    assert_exit "promote node1" "0" "$rc"

    # Explicitly reset chain after promotion
    "$ZEP_BIN" -bw "$DS" --alias node1 --config "chain=node1,node2,node3" --all </dev/null > /dev/null

    chain=$(_get_chain 1)
    assert_out "chain restored" "$chain" "node1,node2,node3"

    out=$(run_zep "$DS" --alias node1 "$LABEL"); rc=$?
    assert_exit "node1 master again exit 0" "0" "$rc"
    assert_out  "node1 shipped" "$out" "Marking sent snapshot"

    out=$(run_zep "zep-node-3/test-3" --alias node3 "$LABEL"); rc=$?
    assert_exit "node3 non-master exit !0" "!0" "$rc"
}

test_non_master() {
    out=$(run_zep "zep-node-2/test-2" --alias node2 "$LABEL"); rc=$?
    assert_exit "exit !0"  "!0" "$rc"
    assert_out  "not master" "$out" "not Master"
}

test_status() {
    destroy_node3; run_zep "$DS" --alias node1 "$LABEL" --init > /dev/null
    out=$(run_zep "$DS" --alias node1 --status); rc=$?
    assert_exit "exit 0"   "0" "$rc"
    assert_out  "node1"    "$out" "node1"
    assert_out  "node2"    "$out" "node2"
    assert_out  "node3"    "$out" "node3"
}

test_rotate() {
    for i in 1 2 3; do clean_tmp; run_zep "$DS" --alias node1 "$LABEL" > /dev/null; done
    run_zep "$DS" --alias node1 --rotate > /dev/null
    cnt=$(zfs list -t snap -H -o name -r "$DS" 2>/dev/null | grep -c "$LABEL" || echo 0)
    if [[ $cnt -le 10 ]]; then
        echo -e "  ${GREEN}PASS${RESET} rotate count $cnt <= 10"; ((PASS++))
    else
        echo -e "  ${RED}FAIL${RESET} rotate count $cnt > 10"; ((FAIL++))
    fi
}

test_foreign_dataset() {
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
}

test_missing_perms() {
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
}

test_missing_pool() {
    destroy_node3; run_zep "$DS" --alias node1 "$LABEL" --init > /dev/null
    zfs unmount zep-node-3/test-3 2>/dev/null || true
    zpool export zep-node-3
    out=$(run_zep "$DS" --alias node1 "$LABEL"); rc=$?
    assert_exit "exit !0"  "!0" "$rc"
    assert_out  "pool msg" "$out" "not found"
    out=$(run_zep "$DS" --alias node1 --status); rc=$?
    assert_exit "status exit !0" "!0" "$rc"
    assert_out  "status missing" "$out" "MISSING"
    zpool import -f -d /tmp/zep-ramdisk zep-node-3 2>/dev/null || true
}

# ── dispatch ─────────────────────────────────────────────

run_test() {
    local num="$1" desc="$2" func="$3"
    if should_run "$num"; then
        TEST_NUM="$num"
        > "/tmp/test${num}.log"
        ALERT_BEFORE=$(_alert_count)
        echo -e "\n${CYAN}[${num}] ${desc}${RESET}"
        "$func" </dev/null
    fi
}

while IFS='|' read -r num desc func; do
    run_test "$num" "$desc" "$func"
done < <(_test_table)

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
