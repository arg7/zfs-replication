#!/bin/bash

# zfs-common.lib.sh - Shared utility functions for Zeplicator

get_zfs_prop() {
    local prop=$1
    local ds=$2
    zfs get -H -o value "$prop" "$ds" 2>/dev/null | grep -v "^-$" | head -n 1
}

# Get the local node alias (using cli override, hostname in chain, fqdn match, or hostname as fallback)
get_local_alias() {
    local raw_ds="$1"
    local cli_alias="$2"

    # 1. Check if CLI alias is provided (Overrides all auto-discovery)
    if [[ -n "$cli_alias" ]]; then
        echo "$cli_alias"
        return 0
    fi

    local sys_host=$(hostname)
    
    # Try to read the chain from the dataset
    local chain=$(get_zfs_prop "repl:chain" "$raw_ds")
    
    if [[ -z "$chain" ]]; then
        # Cannot read chain, fallback to hostname
        echo "$sys_host"
        return 0
    fi

    IFS=',' read -r -a nodes <<< "$chain"

    # 2. Does hostname directly match an alias in the chain?
    for n in "${nodes[@]}"; do
        if [[ "$n" == "$sys_host" ]]; then
            echo "$sys_host"
            return 0
        fi
    done

    # 3. Does any node's FQDN resolve to one of our local IPs?
    local local_ips=$(hostname -I 2>/dev/null || ip -4 addr show 2>/dev/null | grep -oP '(?<=inet\s)\d+(\.\d+){3}')
    for n in "${nodes[@]}"; do
        local n_fqdn=$(get_zfs_prop "repl:node:${n}:fqdn" "$raw_ds")
        if [[ -n "$n_fqdn" && "$n_fqdn" != "-" ]]; then
            # Direct string match with local IPs
            if echo "$local_ips" | grep -qw "$n_fqdn"; then
                echo "$n"
                return 0
            fi
            # DNS Resolution match
            local resolved_ip=$(getent hosts "$n_fqdn" 2>/dev/null | awk '{print $1}' | head -n 1)
            if [[ -n "$resolved_ip" ]] && echo "$local_ips" | grep -qw "$resolved_ip"; then
                echo "$n"
                return 0
            fi
        fi
    done

    # 4. Fallback to system hostname
    echo "$sys_host"
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

# Resolve SSH timeout (default 10s)
resolve_ssh_timeout() {
    local ds_raw=$1
    local t=$(get_zfs_prop "repl:ssh:timeout" "$ds_raw")
    [[ -z "$t" || "$t" == "-" ]] && echo "10" || echo "$t"
}

# Resolve Process/Job timeout (default 3600s)
resolve_proc_timeout() {
    local ds_raw=$1
    local t=$(get_zfs_prop "repl:timeout" "$ds_raw")
    [[ -z "$t" || "$t" == "-" ]] && echo "3600" || echo "$t"
}

# Resolve pool (filesystem) for a specific node alias
log_message() {
    local msg="$1"
    local alias=$(hostname)
    local log_file="/var/log/zeplicator-${alias}.log"
    echo "$(date '+%Y-%m-%d %H:%M:%S') [$alias] $msg" >> "$log_file" 2>/dev/null || true
}

resolve_node_pool() {
    local alias=$1
    local ds_raw=$2
    local pool=""
    local my_alias=$(get_local_alias "$ds_raw" "")
    
    local fqdn=$(resolve_node_fqdn "$alias" "$ds_raw")
    local user=$(resolve_node_user "$alias" "$ds_raw")
    local ssh_t=$(resolve_ssh_timeout "$ds_raw")

    # Pre-flight check: Is node reachable?
    if [[ "$alias" != "$my_alias" ]]; then
        if ! ssh -o ConnectTimeout="$ssh_t" -o BatchMode=yes "${user}@${fqdn}" "true" 2>/dev/null; then
            return 255
        fi
    fi

    # Try to get pool from the node itself
    if [[ "$alias" == "$my_alias" ]]; then
        pool=$(get_zfs_prop "repl:node:${alias}:fs" "$ds_raw")
    else
        pool=$(timeout "$((ssh_t + 5))" ssh -o ConnectTimeout="$ssh_t" "${user}@${fqdn}" "zfs get -H -o value repl:node:${alias}:fs $ds_raw 2>/dev/null | grep -v '^-' | head -n 1")
    fi
    
    if [[ -z "$pool" ]]; then
        # Fallback 1: try generic 'pool'
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

resolve_node_dataset() {
    local alias=$1
    local ds_raw=$2
    local pool=$(resolve_node_pool "$alias" "$ds_raw")
    local ds_name="${ds_raw#*/}"
    
    if [[ "$pool" == *"/"* ]]; then
        echo "$pool"
    else
        echo "${pool}/${ds_name}"
    fi
}

get_repl_props_encoded() {
    local ds=$1
    # Get all repl: properties, filter out node-specific ones like alias and suspend
    local props=$(zfs get all -H -o property,value "$ds" | grep "^repl:" | grep -vE "repl:(alias|suspend)" | awk '{print $1"="$2}' | tr '\n' ';')
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
                echo "${CHAIN_PREFIX}    Updating ${p%%=*} -> $new_val"
                zfs set "$p" "$ds" || echo "${CHAIN_PREFIX}    Warning: Failed to set $p"
            fi
        fi
    done
}

zbud_msg() { 
    local msg="${CHAIN_PREFIX}    $*"
    echo "$msg" 1>&2
    local alias=$(hostname)
    local log_file="/var/log/zeplicator-${alias}.log"
    # Strip ANSI colors/formatting for the log file
    echo "$(date '+%Y-%m-%d %H:%M:%S') [$alias] $msg" | sed 's/\x1b\[[0-9;]*m//g' >> "$log_file" 2>/dev/null || true
}
zbud_warn() { zbud_msg "⚠️  WARNING: $*"; }

indent_output() {
    sed "s/^/${CHAIN_PREFIX}        /"
}

die() {
    local msg="$1"
    local exit_code=${2:-1}
    zbud_msg "❌ ERROR: $msg"

    if [[ -n "$local_ds" ]]; then
        if type send_smtp_alert >/dev/null 2>&1; then
            send_smtp_alert "ERROR: $msg"
        fi
    fi
    if [[ "$CASCADED" != true && "$exit_code" -eq 2 ]]; then
        echo "HINT: If replication failed due to divergent snapshots, try recovery options:"
        echo "  --promote --auto [-y]         (Auto-discover latest common snapshot and rollback chain)"
        echo "  --promote --snap <name> [-y]  (Rollback chain to specific snapshot)"
        echo "  --promote --destroy-chain     (DANGER: Destroy downstream datasets and start over)"
    fi
    exit $exit_code
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
    local lock_suffix=""
    [[ -n "$CLI_ALIAS" ]] && lock_suffix="-${CLI_ALIAS}"
    local lock_name="${dataset//\//-}-${label}${lock_suffix}.lock"
    LOCKFILE="/tmp/${lock_name}"
    
    local timeout_val=$(resolve_proc_timeout "$dataset")
    
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
            if [[ $waited -ge $timeout_val ]]; then
                die "ERR: Timeout waiting for lock $LOCKFILE after $timeout_val seconds. Lock held by PID: $lock_pid"
            fi
            echo "Lock $LOCKFILE held by PID $lock_pid. Waiting... ($waited/${timeout_val}s)"
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
                send_smtp_alert "CRITICAL: ZFS replication job for $dataset ($label) is stuck. Lock file: $LOCKFILE. Age: $((age/60)) min. Timeout: $((timeout_val/60)) min. PID recorded: $lock_pid"
            fi
            die "ERR: Stuck job detected ($age seconds old) at $LOCKFILE. Alert sent."
        else
            die "ERR: Replication already running ($age seconds ago) at $LOCKFILE. PID: $lock_pid"
        fi
    done

    echo "$$" > "$LOCKFILE"
    trap 'rm -f "$LOCKFILE"' EXIT
}
