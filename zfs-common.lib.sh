#!/bin/bash

# zfs-common.lib.sh - Shared utility functions for Zeplicator

get_zfs_prop() {
    local prop=$1
    local ds=$2
    zfs get -H -o value "$prop" "$ds" 2>/dev/null | grep -v "^-$" | head -n 1
}

# Resolve FQDN/Address for a specific node alias
resolve_node_fqdn() {
    local alias=$1
    local ds_raw=$2
    local fqdn=$(get_zfs_prop "repl:node:${alias}:fqdn" "$ds_raw")
    [[ -z "$fqdn" ]] && echo "$alias" || echo "$fqdn"
}

# Resolve SSH user for a specific node alias
resolve_node_user() {
    local alias=$1
    local ds_raw=$2
    local user=$(get_zfs_prop "repl:node:${alias}:user" "$ds_raw")
    if [[ -z "$user" ]]; then
        user=$(get_zfs_prop "repl:user" "$ds_raw")
    fi
    [[ -z "$user" ]] && echo "root" || echo "$user"
}

# Resolve pool (filesystem) for a specific node alias
resolve_node_pool() {
    local alias=$1
    local ds_raw=$2
    local pool=""
    local my_alias=$(hostname) # Assuming hostname matches the alias in the chain
    
    # Pre-flight check: Is node reachable?
    if [[ "$alias" != "$my_alias" ]]; then
        local fqdn=$(resolve_node_fqdn "$alias" "$ds_raw")
        local user=$(resolve_node_user "$alias" "$ds_raw")
        if ! ssh -o ConnectTimeout=5 -o BatchMode=yes "${user}@${fqdn}" "true" 2>/dev/null; then
            return 255
        fi
    fi

    # 1. Check namespaced property: repl:node:<alias>:fs
    pool=$(get_zfs_prop "repl:node:${alias}:fs" "$ds_raw")
    
    # 2. Check local properties if we are on that node (fallback search)
    if [[ -z "$pool" && "$alias" == "$my_alias" ]]; then
        pool=$(get_zfs_prop "repl:node:${alias}:fs" "${alias}-pool/${ds_raw#*/}")
    fi
    
    if [[ -z "$pool" ]]; then
        # Fallback 1: try generic 'pool'
        local fqdn=$(resolve_node_fqdn "$alias" "$ds_raw")
        local user=$(resolve_node_user "$alias" "$ds_raw")
        if [[ "$alias" == "$my_alias" ]]; then
            if zfs list pool >/dev/null 2>&1; then pool="pool"; fi
        else
            if ssh "${user}@${fqdn}" "zfs list pool >/dev/null 2>&1"; then pool="pool"; fi
        fi
        
        # Fallback 2: classic naming
        [[ -z "$pool" ]] && pool="${alias}-pool"
    fi
    echo "$pool"
}

get_repl_props_encoded() {
    local ds=$1
    # Get all repl: properties, format as key=value, semicolon separated, then base64
    local props=$(zfs get all -H -o property,value "$ds" | grep "^repl:" | awk '{print $1"="$2}' | tr '\n' ';')
    echo -n "$props" | base64 -w 0
}

apply_repl_props() {
    local ds=$1
    local encoded=$2
    [[ -z "$encoded" ]] && return
    
    echo "${CHAIN_PREFIX}  ⚙️  Syncing replication properties for $ds..."
    local decoded=$(echo -n "$encoded" | base64 -d)
    IFS=';' read -ra props <<< "$decoded"
    for p in "${props[@]}"; do
        if [[ -n "$p" ]]; then
            local current_val=$(zfs get -H -o value "${p%%=*}" "$ds" 2>/dev/null)
            local new_val="${p#*=}"
            if [[ "$current_val" != "$new_val" ]]; then
                echo "  Updating ${p%%=*} -> $new_val"
                zfs set "$p" "$ds" || echo "  Warning: Failed to set $p"
            fi
        fi
    done
}

zbud_msg() { echo "${CHAIN_PREFIX}    $*" 1>&2; }
zbud_warn() { zbud_msg "⚠️  WARNING: $*"; }

indent_output() {
    sed "s/^/${CHAIN_PREFIX}        /"
}

die() {
    zbud_msg "❌ ERROR: $*"

    if [[ -n "$dataset" ]]; then
        if type send_smtp_alert >/dev/null 2>&1; then
            send_smtp_alert "ERROR in ZFSBUD: $*"
        fi
    fi
    echo "HINT: If replication failed due to divergent snapshots, try recovery options:"
    echo "  --promote --auto [-y]         (Auto-discover latest common snapshot and rollback chain)"
    echo "  --promote --snap <name> [-y]  (Rollback chain to specific snapshot)"
    echo "  --promote --destroy-chain     (DANGER: Destroy downstream datasets and start over)"
    exit 1
}

die() {
    echo "$@"
    if [[ -n "$dataset" ]]; then
        if type send_smtp_alert >/dev/null 2>&1; then
            send_smtp_alert "ERROR: $*"
        fi
    fi
    echo "HINT: If replication failed due to divergent snapshots, try recovery options:"
    echo "  --promote --auto [-y]         (Auto-discover latest common snapshot and rollback chain)"
    echo "  --promote --snap <name> [-y]  (Rollback chain to specific snapshot)"
    echo "  --promote --destroy-chain     (DANGER: Destroy downstream datasets and start over)"
    exit 1
}

zbud_config_read_file() {
  (grep -E "^${2}=" -m 1 "${1}" 2>/dev/null || echo "VAR=__UNDEFINED__") | head -n 1 | cut -d '=' -f 2-;
}

zbud_config_get() {
  local working_dir="$(dirname "$(readlink -f "$0")")"
  local val="$(zbud_config_read_file $working_dir/zfsbud.conf "${1}")";
  if [ "${val}" = "__UNDEFINED__" ]; then
    val="$(zbud_config_read_file $working_dir/default.zfsbud.conf "${1}")";
    if [ "${val}" = "__UNDEFINED__" ]; then
      # Fallback defaults if config files are missing
      case "$1" in
        default_snapshot_prefix) echo "zfsbud_" ;;
        src_minutes|src_hourly|src_daily|src_weekly|src_monthly|src_yearly) echo "0" ;;
        dst_minutes|dst_hourly|dst_daily|dst_weekly|dst_monthly|dst_yearly) echo "0" ;;
        *) zbud_die "Default configuration for '${1}' is missing." ;;
      esac
      return
    fi
  fi
  printf -- "%s" "${val}";
}

check_stuck_job() {
    local lock_name="${dataset//\//-}-${label}.lock"
    LOCKFILE="/tmp/${lock_name}"
    
    local timeout_val=$(get_zfs_prop "repl:timeout" "$dataset")
    [[ -z "$timeout_val" ]] && timeout_val="3600"
    
    # Determine if we should wait or fail fast
    # Wait for: promote, cascaded, suspend, resume, mark-only, or manual run (terminal)
    local wait_for_lock=false
    if [[ "$CASCADED" == true || "$PROMOTE" == true || "$SUSPEND" == true || "$RESUME" == true || "$MARK_ONLY" == true || -t 0 ]]; then
        wait_for_lock=true
    fi

    local waited=0
    while [[ -f "$LOCKFILE" ]]; do
        local lock_pid=$(cat "$LOCKFILE" 2>/dev/null)
        
        if [[ "$wait_for_lock" == true ]]; then
            if [[ $waited -ge 300 ]]; then
                die "ERR: Timeout waiting for lock after 5 minutes. Lock held by PID: $lock_pid"
            fi
            echo "Lock held by PID $lock_pid. Waiting... ($waited/300s)"
            sleep 10
            waited=$((waited + 10))
            continue
        fi

        # Normal cron behavior (fail fast or check stuck)
        local cur_time=$(date +%s)
        local m_time=$(stat -c %Y "$LOCKFILE" 2>/dev/null || echo "$cur_time")
        local age=$((cur_time - m_time))
        
        if [[ "$age" -gt "$timeout_val" ]]; then
            if type send_smtp_alert >/dev/null 2>&1; then
                send_smtp_alert "CRITICAL: ZFS replication job for $dataset ($label) is stuck. Lock file age: $((age/60)) min. Timeout: $((timeout_val/60)) min. PID recorded: $lock_pid"
            fi
            die "ERR: Stuck job detected ($age seconds old). Alert sent."
        else
            die "ERR: Replication already running ($age seconds ago). PID: $lock_pid"
        fi
    done

    echo "$$" > "$LOCKFILE"
    trap 'rm -f "$LOCKFILE"' EXIT
}
