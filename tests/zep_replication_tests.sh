#!/bin/bash

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
CONF_FILE="$SCRIPT_DIR/test.conf"
[[ -f "$CONF_FILE" ]] && source "$CONF_FILE"

ZEP_BIN=$(command -v zep)
DS="zep-node-1/test-1"
LABEL="min1"

# ── sanity: apply default config to all nodes ─────────────
"$ZEP_BIN" -bw "$DS" --alias node1 --config --import "$CONF_FILE" </dev/null > /dev/null 2>&1 || true

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
17|LOST COMMON / DONOR RECOVERY|test_lost_common_donor
18|FOREIGN DATASET|test_foreign_dataset
19|MISSING PERMISSIONS|test_missing_perms
20|MISSING POOL|test_missing_pool
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

_fail_log() {
    local sub=$((++SUB))
    cp "/tmp/test${TEST_NUM:-00}.log" "/tmp/test${TEST_NUM:-00}-${sub}.log"
    echo -e ", ${C_DIM}see log ${TEST_NUM:-00}-${sub}${C_RESET}"
}

assert_exit() {
    local name="$1" expected="$2" actual="$3"
    if [[ "$expected" == "0" && "$actual" -eq 0 ]] || [[ "$expected" == "!0" && "$actual" -ne 0 ]] || [[ "$expected" =~ ^[0-9]+$ && "$actual" -eq "$expected" ]]; then
        echo -e "  ${GREEN}PASS${RESET} $name"
        ((PASS++))
    else
        echo -ne "  ${RED}FAIL${RESET} $name (expected ${expected}, got ${actual})"
        _fail_log
        ((FAIL++))
    fi
}

assert_out() {
    local name="$1" output="$2" pattern="$3" expect="${4:-yes}"
    if echo "$output" | grep -qF "$pattern"; then
        if [[ "$expect" == "yes" ]]; then
            echo -e "  ${GREEN}PASS${RESET} $name"; ((PASS++))
        else
            echo -ne "  ${RED}FAIL${RESET} $name (unexpected match: $pattern)"; _fail_log; ((FAIL++))
        fi
    else
        if [[ "$expect" == "yes" ]]; then
            echo -ne "  ${RED}FAIL${RESET} $name (missing: $pattern)"; _fail_log; ((FAIL++))
        else
            echo -e "  ${GREEN}PASS${RESET} $name"; ((PASS++))
        fi
    fi
}

assert_ge() {
    local name="$1" actual="$2" expected="$3"
    if [[ "$actual" -ge "$expected" ]]; then
        echo -e "  ${GREEN}PASS${RESET} $name ($actual >= $expected)"; ((PASS++))
    else
        echo -ne "  ${RED}FAIL${RESET} $name ($actual < $expected)"; _fail_log; ((FAIL++))
    fi
}

assert_eq() {
    local name="$1" expected="$2" actual="$3"
    if [[ "$actual" == "$expected" ]]; then
        echo -e "  ${GREEN}PASS${RESET} $name ($actual)"; ((PASS++))
    else
        echo -ne "  ${RED}FAIL${RESET} $name (expected $expected, got $actual)"; _fail_log; ((FAIL++))
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

# ── ZFS type detection ────────────────────────────────────
# zfs-fuse: no kernel module, no zfs allow, no sudo needed
# kernel ZFS: /sys/module/zfs exists, zfs allow works, sudo for admin ops
ZFS_FUSE=false
[[ -d /sys/module/zfs ]] || ZFS_FUSE=true

# ── remote SSH helpers ───────────────────────────────────
# All ZFS operations on node2/node3 go through SSH.
# zep-user-$i accounts have delegated ZFS perms (kernel) or raw access (fuse).
# On zfs-fuse _ssh_node_sudo falls back to _ssh_node (no sudo needed).

_ssh_node() {
    local node="$1" cmd="$2"
    ssh -n "zep-user-${node}@zep-node-${node}.local" "$cmd" 2>/dev/null
}

_ssh_node_sudo() {
    local node="$1" cmd="$2"
    if [[ "$ZFS_FUSE" == "true" ]]; then
        _ssh_node "$node" "$cmd"
    else
        ssh -n "zep-user-${node}@zep-node-${node}.local" "sudo $cmd" 2>/dev/null
    fi
}

destroy_node3() { _ssh_node 3 "zfs destroy -r zep-node-3/test-3" 2>/dev/null || true; }

_guid_of_snap() {
    local ds="$1" snap="$2"
    zfs get -H -o value guid "${ds}@${snap}" 2>/dev/null
}

_verify_guid_on_sink() {
    local desc="$1" expected_guid="$2" sink_ds="$3"
    local node
    [[ "$sink_ds" =~ zep-node-([0-9]+)/ ]] && node="${BASH_REMATCH[1]}"
    local found
    found=$(_ssh_node "${node:-3}" "zfs list -t snap -H -o guid ${sink_ds} 2>/dev/null | grep -c '^${expected_guid}\$'" || true)
    if [[ "$found" -ge 1 ]]; then
        echo -e "  ${GREEN}PASS${RESET} $desc (GUID $expected_guid on $sink_ds)"; ((PASS++))
    else
        echo -ne "  ${RED}FAIL${RESET} $desc (GUID $expected_guid not on $sink_ds)"; _fail_log; ((FAIL++))
    fi
}

_pre_test_cleanup() {
    # Sim-only: re-import any exported pools (harmless if already imported)
    for i in 2 3; do
        zpool import -f -d /tmp/zep-ramdisk "zep-node-$i" 2>/dev/null || true
    done
    _ssh_node 2 "zfs destroy -r zep-node-2/test-2" 2>/dev/null || true
    _ssh_node 3 "zfs destroy -r zep-node-3/test-3" 2>/dev/null || true
    sleep 1
    # Re-ensure pool-level permissions survive dataset destruction
    zfs allow zep-user-2 create,mount,receive,destroy,send,snapshot,hold,release,userprop,diff zep-node-2 2>/dev/null || true
    zfs allow zep-user-3 create,mount,receive,destroy,send,snapshot,hold,release,userprop,diff zep-node-3 2>/dev/null || true
    # Restore FQDNs and hosts for all nodes (undo any prior isolation)
    for i in 1 2 3; do
        zfs set "zep:node:node${i}:fqdn=zep-node-${i}.local" "$DS" 2>/dev/null || true
    done
    "$ZEP_BIN" -bw "$DS" --alias node1 --config policy=fail chain=node1,node2,node3 zep:zfs:recv_opt=- zep:debug:send_maxbytes=- zep:debug:throttle=- zep:debug:send_timeout=0 </dev/null > /dev/null 2>&1
}

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
        echo -ne "  ${RED}FAIL${RESET} alert: $desc (missing: $pattern)"
        _fail_log
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
    zfs set zep:debug:send_maxbytes=1M "$DS"
    "$ZEP_BIN" -bw "$DS" --alias node1 --config policy=fail --all </dev/null > /dev/null 2>&1
    "$ZEP_BIN" -bw "$DS" --alias node1 --config zep:zfs:recv_opt="-F -s" --all </dev/null > /dev/null 2>&1
}
teardown_resume_mode() {
    zfs inherit zep:debug:send_maxbytes "$DS" 2>/dev/null || zfs set zep:debug:send_maxbytes=- "$DS"
    zfs inherit zep:debug:throttle "$DS" 2>/dev/null || zfs set zep:debug:throttle=- "$DS"
    zfs inherit zep:debug:send_timeout "$DS" 2>/dev/null || zfs set zep:debug:send_timeout=0 "$DS"
    "$ZEP_BIN" -bw "$DS" --alias node1 --config zep:debug:send_maxbytes=- --all </dev/null > /dev/null 2>&1
    "$ZEP_BIN" -bw "$DS" --alias node1 --config zep:debug:throttle=- --all </dev/null > /dev/null 2>&1
    "$ZEP_BIN" -bw "$DS" --alias node1 --config zep:debug:send_timeout=0 --all </dev/null > /dev/null 2>&1
}

isolate_node() {
    local node="$1"
    local ds="zep-node-1/test-1"
    zfs set "zep:node:node${node}:fqdn=zep-node-${node}.local.disabled" "$ds"
}

restore_node() {
    local node="$1"
    local ds="zep-node-1/test-1"
    zfs set "zep:node:node${node}:fqdn=zep-node-${node}.local" "$ds"
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

_ensure_mounted() {
    local ds="$1"
    local node=1
    [[ "$ds" =~ zep-node-([0-9]+)/ ]] && node="${BASH_REMATCH[1]}"

    local mounted mount_ok=0
    if [[ "$node" -eq 1 ]]; then
        mounted=$(zfs get -H -o value mounted "$ds" 2>/dev/null)
    else
        mounted=$(zfs get -H -o value mounted "$ds" 2>/dev/null)
    fi
    if [[ "$mounted" == "yes" ]]; then
        return 0
    fi
    if [[ "$node" -eq 1 ]]; then
        zfs set canmount=on "$ds" 2>/dev/null && zfs mount "$ds" 2>/dev/null && mount_ok=1
    else
        zfs set canmount=on "$ds" 2>/dev/null && zfs mount "$ds" 2>/dev/null && mount_ok=1
    fi
    if [[ $mount_ok -eq 1 ]]; then
        sleep 0.5
        return 0
    fi
    echo -ne "  ${RED}FAIL${RESET} $ds not mounted (can't write)"
    _fail_log
    ((FAIL++))
    return 1
}

_write_error() {
    local node="$1"
    local ds="zep-node-${node}/test-${node}"
    _ensure_mounted "$ds" || return 1
    echo "divergent: $(date)" >> "/${ds}/error" && sync && sleep 1
    zfs unmount ${ds} 2>/dev/null; zfs set canmount=noauto ${ds} 2>/dev/null; true
}

_rollback_node() {
    local node="$1"
    local snap
    snap=$(_ssh_node "$node" "zfs list -t snap -o name -H -S creation zep-node-${node}/test-${node} 2>/dev/null | grep '@zep_' | head -1 | cut -d@ -f2")
    if [[ -z "$snap" ]]; then
        echo "  ⚠️  No snapshot found on node${node}"
        return 1
    fi
    _ssh_node "$node" "zfs rollback -r zep-node-${node}/test-${node}@${snap}" 2>/dev/null
}

_check_flag() {
    local node="$1" expected="$2"
    local val
    val=$(_ssh_node "$node" "zfs get -H -o value zep:error:split-brain zep-node-${node}/test-${node}" 2>/dev/null)
    if [[ "$val" == "$expected" || ("$val" == "-" && "$expected" == "false") ]]; then
        echo -e "  ${GREEN}PASS${RESET} node${node} split-brain = '$val'"
        ((PASS++))
    else
        echo -ne "  ${RED}FAIL${RESET} node${node} split-brain = '$val' (expected '$expected')"
        _fail_log
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
        zpool import -f "zep-node-$i" 2>/dev/null || true
        zpool destroy -f "zep-node-$i" 2>/dev/null || true
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
    out=$(run_zep "$DS" --alias node1 "$LABEL" --init); rc=$?
    assert_exit "exit 0"   "0" "$rc"
    assert_out  "cascade"  "$out" "VERIFICATION SUCCESS"
    assert_out  "shipped"  "$out" "Marking sent snapshot"
    for i in 1 2 3; do
        local ds_n="zep-node-$i/test-$i"
        local cnt
        if [[ "$i" -eq 1 ]]; then
            cnt=$(zfs list -t snap -H -o name -r "$ds_n" 2>/dev/null | grep -c "$LABEL" || true)
        else
            cnt=$(_ssh_node "$i" "zfs list -t snap -H -o name -r ${ds_n} 2>/dev/null | grep -c ${LABEL}" || true)
        fi
        assert_ge "node$i snaps" "$cnt" 1
    done
    local latest_snap guid
    latest_snap=$(zfs list -t snap -H -o name -S creation "$DS" 2>/dev/null | grep "@zep_" | head -1)
    if [[ -n "$latest_snap" ]]; then
        guid=$(_guid_of_snap "$DS" "${latest_snap#*@}")
        _verify_guid_on_sink "init GUID on node3" "$guid" "zep-node-3/test-3"
    fi
    local alerts; alerts=$(_check_alerts)
    _assert_alert "initial replication" "$alerts" "initial replication successful"
}

test_incremental() {
    run_zep "$DS" --alias node1 "$LABEL" --init > /dev/null
    _ensure_mounted "zep-node-1/test-1" || return 1
    echo "increment" >> /zep-node-1/test-1/inc.dat
    sync
    zfs snapshot "zep-node-1/test-1@zep_${LABEL}_inc_1" 2>/dev/null
    sync
    local latest_snap guid
    latest_snap=$(zfs list -t snap -H -o name -S creation "$DS" 2>/dev/null | grep "@zep_" | head -1)
    guid=$(_guid_of_snap "$DS" "${latest_snap#*@}")

    out=$(run_zep "$DS" --alias node1 "$LABEL"); rc=$?
    assert_exit "exit 0"   "0" "$rc"
    assert_out  "cascade"  "$out" "VERIFICATION SUCCESS"
    _verify_guid_on_sink "inc GUID on node3" "$guid" "zep-node-3/test-3"
}

test_divergence() {
    run_zep "$DS" --alias node1 "$LABEL" --init > /dev/null
    local ds3="zep-node-3/test-3"

    # Scenario 1: GUID mismatch — same name, different GUID on destination
    local dup_snap=$(_ssh_node 3 "zfs list -t snap -H -o name -S creation ${ds3} | head -1")
    local snap_short="${dup_snap##*@}"
    _ssh_node 3 "zfs destroy ${dup_snap}" 2>/dev/null
    _ssh_node 3 "zfs snapshot ${ds3}@tmp_guid && zfs rename ${ds3}@tmp_guid ${ds3}@${snap_short}"

    out=$(run_zep "$DS" --alias node1 "$LABEL" --init); rc=$?
    assert_exit "guid mismatch !0"  "!0" "$rc"
    assert_out  "no common ground"   "$out" "no common ground"

    # Clean up: destroy node3 dataset, let re-init recreate it
    _ssh_node 3 "zfs destroy -r ${ds3}" 2>/dev/null || true
    run_zep "$DS" --alias node1 "$LABEL" --init > /dev/null

    # Scenario 2: data divergence — write to destination
    _ensure_mounted "$ds3" || return 1
    dd if=/dev/urandom of=/${ds3}/div.bin bs=64K count=4 conv=fsync 2>/dev/null
    sync; sleep 1
    zfs unmount ${ds3} 2>/dev/null; zfs set canmount=noauto ${ds3} 2>/dev/null; true

    out=$(run_zep "$DS" --alias node1 "$LABEL" --init); rc=$?
    assert_exit "data diverge !0"  "!0" "$rc"
    assert_out  "split-brain"      "$out" "Split-Brain detected"
    local alerts; alerts=$(_check_alerts)
    _assert_alert "split-brain" "$alerts" "split-brain detected"
}

test_divergence_override() {
    run_zep "$DS" --alias node1 "$LABEL" --init > /dev/null
    local ds3="zep-node-3/test-3"
    _ensure_mounted "$ds3" || return 1
    echo diverged >> /${ds3}/div.dat && sync && sleep 1
    zfs unmount ${ds3} 2>/dev/null; zfs set canmount=noauto ${ds3} 2>/dev/null; true

    zfs set zep:zfs:recv_opt=-F "$DS"
    out=$(run_zep "$DS" --alias node1 "$LABEL"); rc=$?
    zfs inherit zep:zfs:recv_opt "$DS" 2>/dev/null || zfs set zep:zfs:recv_opt=- "$DS"
    assert_exit "exit 0"  "0" "$rc"
}

test_resume() {
    run_zep "$DS" --alias node1 "$LABEL" --init > /dev/null

    setup_resume_mode

    # Write enough data to trigger a transfer that may resume (~2MB)
    _ensure_mounted "zep-node-1/test-1" || return 1
    dd if=/dev/urandom of=/zep-node-1/test-1/resume_big.dat bs=1M count=2 conv=fsync 2>/dev/null
    sync; sleep 1

    # Run — expect interruption at 1MB
    out=$(run_zep "$DS" --alias node1 "$LABEL"); rc=$?
    assert_exit "interrupted" "!0" "$rc"
    assert_out  "max-bytes msg" "$out" "iomon: max-bytes"

    # Retry until complete — disable max_bytes so resume can finish
    zfs inherit zep:debug:send_maxbytes "$DS" 2>/dev/null || zfs set zep:debug:send_maxbytes=- "$DS"
    completed=false
    for attempt in $(seq 1 30); do
        clean_tmp
        out=$(run_zep "$DS" --alias node1 "$LABEL"); rc=$?
        if [[ $rc -eq 0 ]]; then
            completed=true
            break
        fi
    done
    if [[ "$completed" == true ]]; then
        echo -e "  ${GREEN}PASS${RESET} completed within 30 retries"; ((PASS++))
    else
        echo -ne "  ${RED}FAIL${RESET} did not complete in 30 retries"; _fail_log; ((FAIL++))
    fi

    teardown_resume_mode
}

test_resume_failed() {
    run_zep "$DS" --alias node1 "$LABEL" --init > /dev/null

    setup_resume_mode

    # Create 5 snapshots with ~2MB data each
    _ensure_mounted "zep-node-1/test-1" || return 1
    for i in $(seq 1 5); do
        dd if=/dev/urandom of=/zep-node-1/test-1/snap_${i}.dat bs=1M count=2 conv=fsync 2>/dev/null
        sync; sleep 1
        zfs snapshot "zep-node-1/test-1@zep_${LABEL}_snap_${i}" 2>/dev/null
    done

    # Run — will be interrupted, token saved on node2
    out=$(run_zep "$DS" --alias node1 "$LABEL"); rc=$?
    assert_exit "interrupted" "!0" "$rc"
    assert_out  "max-bytes msg" "$out" "iomon: max-bytes"

    # Verify resume token exists on node2
    token=$(_ssh_node 2 "zfs get -H -o value receive_resume_token zep-node-2/test-2" 2>/dev/null || echo "-")
    if [[ "$token" != "-" ]]; then
        echo -e "  ${GREEN}PASS${RESET} resume token saved"; ((PASS++))
    else
        echo -ne "  ${RED}FAIL${RESET} resume token not saved"; _fail_log; ((FAIL++))
    fi

    # Destroy snapshots 1-4 on master (being transmitted)
    for i in 1 2 3 4; do
        zfs destroy "zep-node-1/test-1@zep_${LABEL}_snap_${i}" 2>/dev/null || true
    done

    # Re-run — should detect ERR_RESUME_FAILED, clear token
    out=$(run_zep "$DS" --alias node1 "$LABEL"); rc=$?
    assert_exit "resume failed" "!0" "$rc"
    assert_out  "token invalidated" "$out" "Resume token invalidated"
    assert_out  "token destroyed" "$out" "Destroyed stale resume token"

    # Token should be cleared
    token=$(_ssh_node 2 "zfs get -H -o value receive_resume_token zep-node-2/test-2" 2>/dev/null || echo "-")
    if [[ "$token" == "-" ]]; then
        echo -e "  ${GREEN}PASS${RESET} token cleared after failure"; ((PASS++))
    else
        echo -ne "  ${RED}FAIL${RESET} token not cleared"; _fail_log; ((FAIL++))
    fi

    # Disable throttling, run clean — should complete with --init for node3
    teardown_resume_mode
    out=$(run_zep "$DS" --alias node1 "$LABEL"); rc=$?
    assert_exit "clean run after failure" "0" "$rc"

    # Sink must have at least the new snapshot
    rem=$(_ssh_node 3 "zfs list -t snap -H -o name zep-node-3/test-3 2>/dev/null | grep -c 'zep_${LABEL}'" 2>/dev/null || true)
    rem=$(echo "$rem" | tr -d '[:space:]')
    if [[ -n "$rem" && "$rem" -ge 1 ]]; then
        echo -e "  ${GREEN}PASS${RESET} sink snapshots: $rem >= 1"; ((PASS++))
    else
        echo -ne "  ${RED}FAIL${RESET} sink snapshots: '$rem' < 1"; _fail_log; ((FAIL++))
    fi
}

test_resilience_offline() {
    run_zep "$DS" --alias node1 "$LABEL" --init > /dev/null

    # Set policy=resilience on master
    "$ZEP_BIN" -bw "$DS" --alias node1 --config policy=resilience </dev/null > /dev/null

    # Isolate node2 (middle in chain: node1→node2→node3, master skips to node3)
    isolate_node 2

    for cycle in 1 2 3; do
        out=$(run_zep "$DS" --alias node1 "$LABEL"); rc=$?
        assert_exit "cycle $cycle exit 3" "3" "$rc"
        assert_out  "skip node2" "$out" "Replication to node2 failed"
    done

    # Verify node3 still receives snapshots even though node2 is down
    snap_cnt=$(_ssh_node 3 "zfs list -t snap -H -o name -r zep-node-3/test-3 2>/dev/null | grep -c ${LABEL}" || true)
    assert_ge "node3 got snaps while node2 offline" "$snap_cnt" 4
}

test_resilience_recovery() {
    run_zep "$DS" --alias node1 "$LABEL" --init > /dev/null
    "$ZEP_BIN" -bw "$DS" --alias node1 --config policy=resilience </dev/null > /dev/null
    isolate_node 2
    for cycle in 1 2 3; do
        run_zep "$DS" --alias node1 "$LABEL" > /dev/null
    done
    restore_node 2
    sleep 1

    out=$(run_zep "$DS" --alias node1 "$LABEL"); rc=$?
    assert_exit "restored exit 0" "0" "$rc"
    assert_out  "cascade ok" "$out" "VERIFICATION SUCCESS"

    # Reset policy to fail for subsequent test runs
    "$ZEP_BIN" -bw "$DS" --alias node1 --config policy=fail </dev/null > /dev/null
}

test_splitbrain_resilience() {
    run_zep "$DS" --alias node1 "$LABEL" --init > /dev/null
    "$ZEP_BIN" -bw "$DS" --alias node1 --config policy=fail --all </dev/null > /dev/null

    _write_error 2
    out=$(run_zep "$DS" --alias node1 "$LABEL"); rc=$?
    assert_exit "split-brain node2 exit 2" "2" "$rc"
    assert_out  "split-brain msg" "$out" "Split-Brain detected"
    _check_flag 2 "true"

    # Resilience: skip diverged node
    "$ZEP_BIN" -bw "$DS" --alias node1 --config policy=resilience --all </dev/null > /dev/null
    out=$(run_zep "$DS" --alias node1 "$LABEL"); rc=$?
    assert_exit "resilience skip exit 3" "3" "$rc"
    assert_out  "resilience skip" "$out" "Skipping due to policy=resilience"
    _check_flag 2 "true"
    local alerts; alerts=$(_check_alerts)
    _assert_alert "split-brain detected" "$alerts" "split-brain detected"
    _assert_alert "split-brain skipped"   "$alerts" "split-brain skipped"
}

test_splitbrain_rollback() {
    run_zep "$DS" --alias node1 "$LABEL" --init > /dev/null
    "$ZEP_BIN" -bw "$DS" --alias node1 --config policy=fail </dev/null > /dev/null
    _write_error 2
    out=$(run_zep "$DS" --alias node1 "$LABEL"); rc=$?
    assert_exit "split-brain node2 exit 2" "2" "$rc"
    _check_flag 2 "true"

    _rollback_node 2
    _check_flag 2 "true"  # flag persists until successful replication

    out=$(run_zep "$DS" --alias node1 "$LABEL"); rc=$?
    assert_exit "rollback recovery exit 0" "0" "$rc"
    assert_out  "cascade ok" "$out" "VERIFICATION SUCCESS"
    _check_flag 2 "false"
}

test_divergence_report() {
    run_zep "$DS" --alias node1 "$LABEL" --init > /dev/null
    _write_error 2

    local snap
    snap=$(_ssh_node 2 "zfs list -t snap -o name -H -S creation zep-node-2/test-2 2>/dev/null | grep '@zep_' | head -1")

    out=$(run_zep "zep-node-2/test-2" --alias node2 --divergence-report "${snap#*@}"); rc=$?
    assert_exit "divergence-report exit 2" "2" "$rc"
    assert_out  "report has details" "$out" "+	/zep-node-2/test-2/error"
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
    destroy_node3
    run_zep "$DS" --alias node1 "$LABEL" --init > /dev/null
    _promote 3; rc=$?
    assert_exit "promote node3" "0" "$rc"
    chain=$(_get_chain 1)
    assert_out "chain node3,node1,node2" "$chain" "node3,node1,node2"

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
    run_zep "$DS" --alias node1 "$LABEL" --init > /dev/null
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
    # Disable cron rotation on all nodes (interferes with counting)
    local cron_saved=$(crontab -l 2>/dev/null)
    crontab -l 2>/dev/null | sed 's/^\(.*--rotate.*\)/#\1/' | crontab -
    trap '[[ -n "$cron_saved" ]] && echo "$cron_saved" | crontab -' RETURN

    run_zep "$DS" --alias node1 "$LABEL" --init > /dev/null
    for i in 1 2 3; do clean_tmp; run_zep "$DS" --alias node1 "$LABEL" > /dev/null; done

    # Create 3 manual (unshipped) snapshots
    for i in 1 2 3; do
        zfs snapshot "${DS}@zep_${LABEL}_manual_${i}" 2>/dev/null
    done

    # Set retention=2 for this label on master
    zfs set "zep:role:master:keep:${LABEL}=2" "$DS"

    run_zep "$DS" --alias node1 --rotate > /dev/null

    # Shipped snapshots should be purged; 3 manual unshipped should remain
    local manual=$(zfs list -t snap -H -o name -r "$DS" 2>/dev/null | grep -c "zep_${LABEL}_manual" || true)
    assert_eq "rotate: 3 manual remain" "3" "$manual"

    # Cleanup: restore retention to default 10
    zfs set "zep:role:master:keep:${LABEL}=10" "$DS"
}

test_lost_common_donor() {
    # Scenario: node2 goes offline, master replicates to node3 with aggressive
    # rotation (keep=2). Node2's last_snapshot GUID is preserved on master and sink
    # as common ground. When node2 returns, replication succeeds directly (no donor
    # needed because common ground was never lost).

    run_zep "$DS" --alias node1 "$LABEL" --init > /dev/null

    # Disable cron rotation on all nodes (interferes with controlled rotation)
    local cron_saved=$(crontab -l 2>/dev/null)
    crontab -l 2>/dev/null | sed 's/^\(.*--rotate.*\)/#\1/' | crontab -
    trap '[[ -n "$cron_saved" ]] && echo "$cron_saved" | crontab -' RETURN

    # Set aggressive retention (keep=2) on master AND sink so rotation prunes aggressively
    zfs set "zep:role:master:keep:${LABEL}=2" "$DS"
    zfs set "zep:role:sink:keep:${LABEL}=2"   "zep-node-3/test-3" 2>/dev/null || true

    # Enable resilience so master skips offline node2 and goes to node3
    "$ZEP_BIN" -bw "$DS" --alias node1 --config policy=resilience </dev/null > /dev/null

    # Isolate node2
    isolate_node 2

    # Run many cycles: master creates snaps, ships to node3, rotates old shipped.
    # Also rotate on node3 (sink) to verify it preserves node2's last_snapshot.
    # Node2 never receives them — its last_snapshot freezes and must survive rotation.
    for cycle in $(seq 1 8); do
        run_zep "$DS" --alias node1 "$LABEL" > /dev/null
        run_zep "$DS" --alias node1 --rotate > /dev/null
        run_zep "zep-node-3/test-3" --alias node3 --rotate > /dev/null
    done

    # Verify node3 still preserves node2's last common snapshot (init GUID)
    local node2_last_guid
    node2_last_guid=$(zfs get -H -o value "zep:node:node2:last_snapshot" "$DS" 2>/dev/null)
    if [[ -n "$node2_last_guid" && "$node2_last_guid" != "-" ]]; then
        local found_guid
        found_guid=$(_ssh_node 3 "zfs list -t snap -H -o guid -r zep-node-3/test-3 2>/dev/null | grep -c '^${node2_last_guid}\$'" || true)
        assert_ge "sink preserved node2 last_snapshot GUID" "$found_guid" 1
    fi

    # Get latest snapshot GUID on master before recovery
    local latest_master_guid
    latest_master_guid=$(zfs list -t snap -H -o guid -S creation -r "$DS" 2>/dev/null | head -1)

    # Restore node2
    restore_node 2
    sleep 1

    # Master replicates to node2: common ground is preserved, so direct replication
    # succeeds without donor search. Cascade propagates to node3 (sink).
    out=$(run_zep "$DS" --alias node1 "$LABEL"); rc=$?
    assert_exit "recovery exit 0"    "0"  "$rc"
    assert_out  "replication ok"     "$out" "Replication to zep-user-2@"
    assert_out  "cascade ok"         "$out" "VERIFICATION SUCCESS"

    # Latest snapshot on master after recovery must match sink (node3)
    latest_master_guid=$(zfs list -t snap -H -o guid -S creation -r "$DS" 2>/dev/null | head -1)
    local latest_sink_guid
    latest_sink_guid=$(_ssh_node 3 "zfs list -t snap -H -o guid -S creation -r zep-node-3/test-3 2>/dev/null | head -1" || true)
    assert_eq "latest snap on sink matches master" "$latest_master_guid" "$latest_sink_guid"

    # Reset retention and policy for subsequent tests
    zfs set "zep:role:master:keep:${LABEL}=10" "$DS"
    zfs inherit "zep:role:sink:keep:${LABEL}" "zep-node-3/test-3" 2>/dev/null || true
    "$ZEP_BIN" -bw "$DS" --alias node1 --config policy=fail </dev/null > /dev/null
}

test_foreign_dataset() {
    destroy_node3
    _ssh_node 3 "zfs create zep-node-3/test-3"
    _ssh_node 3 "zfs snapshot zep-node-3/test-3@alien_snap"
    _ssh_node 3 "zfs set canmount=noauto zep-node-3/test-3"
    _ssh_node 3 "zfs unmount zep-node-3/test-3" 2>/dev/null || true
    zfs allow zep-user-3 create,mount,receive,destroy,userprop,diff zep-node-3 2>/dev/null || true
    out=$(run_zep "$DS" --alias node1 "$LABEL" --init); rc=$?
    assert_exit "exit !0"  "!0" "$rc"
    assert_out  "FOREIGN"  "$out" "FOREIGN DATASET"
    # cleanup: reset node3 for subsequent tests
    destroy_node3
    _ssh_node 3 "zfs create -o canmount=noauto zep-node-3/test-3"
    _ssh_node 3 "zfs unmount zep-node-3/test-3" 2>/dev/null || true
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
    zfs allow zep-user-3 create,mount,receive,destroy,send,snapshot,hold,release,userprop,diff zep-node-3 2>/dev/null || true
    zfs allow zep-user-3 create,destroy,send,receive,snapshot,hold,release,userprop,diff zep-node-3/test-3 2>/dev/null || true
}

test_missing_pool() {
    destroy_node3; run_zep "$DS" --alias node1 "$LABEL" --init > /dev/null
    _ssh_node 3 "zfs unmount zep-node-3/test-3" 2>/dev/null || true
    zpool export zep-node-3 2>/dev/null || true
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
        SUB=0
        > "/tmp/test${num}.log"
        ALERT_BEFORE=$(_alert_count)
        echo -e "\n${CYAN}[${num}] ${desc}${RESET}"
        _pre_test_cleanup
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
