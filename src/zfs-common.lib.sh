#!/bin/bash

# zfs-common.lib.sh - Shared utility functions for Zeplicator

# Audit logging for ZFS/ZPOOL commands
zfs() {
    log_message "AUDIT: zfs $*"
    command zfs "$@"
}

zpool() {
    log_message "AUDIT: zpool $*"
    command zpool "$@"
}

ssh() {
    log_message "AUDIT: ssh $*"
    command ssh "$@"
}

# Associative array for in-memory property caching (per-run)
declare -A ZEP_PROP_CACHE

# Default values for commonly-used properties (avoids roundtrips for unset props)
declare -A ZEP_PROP_DEFAULTS=(
    ["zep:snap_prefix"]="zep_"
    ["zep:ssh:timeout"]="30"
    ["zep:proc:timeout"]="60"
    ["zep:suspend"]="false"
    ["zep:zfs:send_opt"]=""
    ["zep:zfs:recv_opt"]="-F"
    ["zep:throttle"]="-"
    ["zep:mbuffer_size"]="1G"
    ["zep:debug:send_delay"]="0"
)

# Populate the property cache for a dataset
cache_zfs_props() {
    local ds="$1"
    # Seed defaults first, then override with actual ZFS values
    for key in "${!ZEP_PROP_DEFAULTS[@]}"; do
        ZEP_PROP_CACHE["${ds}:${key}"]="${ZEP_PROP_DEFAULTS[$key]}"
    done
    # Fetch all zep: properties from ZFS (overrides defaults where set)
    while IFS=$'\t' read -r prop val; do
        [[ "$val" == "-" ]] && continue  # keep default for unset
        [[ "$prop" =~ :(shipped|alias|suspend)$ ]] && continue
        ZEP_PROP_CACHE["${ds}:${prop}"]="$val"
    done < <(zfs get all -H -o property,value "$ds" 2>/dev/null | grep "^zep:")
    # Batch-fetch remaining per-node props in one call (chain node props not yet cached)
    local chain="${ZEP_PROP_CACHE["${ds}:zep:chain"]:-}"
    if [[ -n "$chain" ]]; then
        IFS=',' read -ra chain_nodes <<< "$chain"
        local node_props=""
        for n in "${chain_nodes[@]}"; do
            local k_user="${ds}:zep:node:${n}:user"
            local k_fqdn="${ds}:zep:node:${n}:fqdn"
            local k_fs="${ds}:zep:node:${n}:fs"
            [[ -z "${ZEP_PROP_CACHE[$k_user]+x}" ]] && node_props+="zep:node:${n}:user,"
            [[ -z "${ZEP_PROP_CACHE[$k_fqdn]+x}" ]] && node_props+="zep:node:${n}:fqdn,"
            [[ -z "${ZEP_PROP_CACHE[$k_fs]+x}" ]] && node_props+="zep:node:${n}:fs,"
        done
        if [[ -n "$node_props" ]]; then
            node_props="${node_props%,}"  # strip trailing comma
            local vals
            vals=$(zfs get -H -o value "$node_props" "$ds" 2>/dev/null)
            local i=0
            IFS=',' read -ra props_arr <<< "$node_props"
            while IFS= read -r val; do
                [[ -z "$val" ]] && val="-"
                ZEP_PROP_CACHE["${ds}:${props_arr[$i]}"]="$val"
                ((i++))
            done <<< "$vals"
        fi
    fi
}

get_zfs_prop() {
    local prop=$1
    local ds=$2
    local key="${ds}:${prop}"

    # Check in-memory cache first
    if [[ -n "${ZEP_PROP_CACHE[$key]+x}" ]]; then
        echo "${ZEP_PROP_CACHE[$key]}"
        return 0
    fi

    # Cache miss — fetch from ZFS and cache result (including "-" for unset)
    local val
    val=$(zfs get -H -o value "$prop" "$ds" 2>/dev/null | head -n 1)
    [[ -z "$val" ]] && val="-"
    ZEP_PROP_CACHE["$key"]="$val"
    echo "$val"
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
    
    # Try to read the chain from the filesystem
    local chain=$(get_zfs_prop "zep:chain" "$raw_ds")
    
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
        local n_fqdn=$(get_zfs_prop "zep:node:${n}:fqdn" "$raw_ds")
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
    local fqdn=$(get_zfs_prop "zep:node:${alias}:fqdn" "$ds_raw")
    [[ -z "$fqdn" || "$fqdn" == "-" ]] && echo "$alias" || echo "$fqdn"
}

# Resolve SSH user for a specific node alias
resolve_node_user() {
    local alias=$1
    local ds_raw=$2
    local user=$(get_zfs_prop "zep:node:${alias}:user" "$ds_raw")
    if [[ -z "$user" || "$user" == "-" ]]; then
        user=$(get_zfs_prop "zep:user" "$ds_raw")
    fi
    [[ -z "$user" || "$user" == "-" ]] && echo "root" || echo "$user"
}

# Resolve SSH timeout (default from ZEP_PROP_DEFAULTS: 30s)
resolve_ssh_timeout() {
    local ds_raw=$1
    local t
    t=$(get_zfs_prop "zep:ssh:timeout" "$ds_raw")
    [[ -z "$t" || "$t" == "-" ]] && t="${ZEP_PROP_DEFAULTS["zep:ssh:timeout"]}"
    echo "$t"
}

# Resolve process timeout (default from ZEP_PROP_DEFAULTS: 60s)
resolve_proc_timeout() {
    local ds_raw=$1
    local t
    t=$(get_zfs_prop "zep:proc:timeout" "$ds_raw")
    [[ -z "$t" || "$t" == "-" ]] && t="${ZEP_PROP_DEFAULTS["zep:proc:timeout"]}"
    echo "$t"
}

# Resolve snapshot prefix (default: zep_)
get_snap_prefix() {
    local ds="$1"
    local prefix=$(get_zfs_prop "zep:snap_prefix" "$ds")
    if [[ -z "$prefix" || "$prefix" == "-" ]]; then
        echo "${ZEP_SNAP_PREFIX:-zep_}"
    else
        echo "$prefix"
    fi
}

log_message() {
    local msg="$1"
    local alias=${CLI_ALIAS:-$(hostname)}
    local prefix=${REPL_SNAP_PREFIX:-zep_}
    local uid=${REPL_LOG_UID:-$(id -u)}
    local cmd=${REPL_LOG_CMD:-zep}
    local log_file="/tmp/${prefix}${cmd}-${alias}-${uid}.log"
    # Strip ANSI codes, non-ASCII (emojis), and leading space/pipes
    local clean_msg=$(echo -e "$msg" | sed 's/\x1b\[[0-9;]*m//g' | perl -CS -pe 's/[^\x20-\x7E]//g' | sed -e 's/^[[:space:]|]*//')
    if [[ ! "$clean_msg" =~ ^(INFO|WARNING|ERROR|AUDIT|REPLICATION|ROTATION): ]]; then
        clean_msg="INFO: $clean_msg"
    fi
    echo -e "$(date '+%Y-%m-%d %H:%M:%S') [$alias] $clean_msg" >> "$log_file" 2>/dev/null || true
}

parse_time_to_seconds() {
    local time_str="$1"
    local unit="${time_str: -1}"
    local value="${time_str%?}"

    case "$unit" in
        s) echo "$value" ;;
        m) echo "$((value * 60))" ;;
        h) echo "$((value * 3600))" ;;
        d) echo "$((value * 86400))" ;;
        M) echo "$((value * 86400 * 30))" ;;
        Y) echo "$((value * 86400 * 365))" ;;
        *) echo "$time_str" ;; # assume it's already seconds
    esac
}

format_minutes() {
    local mins="$1"
    [[ -z "$mins" || "$mins" == "-" ]] && { echo "-"; return; }
    
    if [[ $mins -lt 60 ]]; then
        echo "${mins}m"
    elif [[ $mins -lt 1440 ]]; then
        echo "$((mins / 60))h $((mins % 60))m"
    else
        echo "$((mins / 1440))d $(( (mins % 1440) / 60 ))h $((mins % 60))m"
    fi
}

resolve_node_pool() {
    local alias=$1
    local ds_raw=$2
    local pool=""
    local my_alias=$(get_local_alias "$ds_raw" "")
    
    local fqdn=$(resolve_node_fqdn "$alias" "$ds_raw")
    local user=$(resolve_node_user "$alias" "$ds_raw")
    local ssh_t=$(resolve_ssh_timeout "$ds_raw")

    # Try local property first (Master has the full config)
    pool=$(get_zfs_prop "zep:node:${alias}:fs" "$ds_raw")
    
    if [[ -z "$pool" || "$pool" == "-" ]]; then
        # Try to get pool from the node itself if reachable
        if [[ "$alias" != "$my_alias" ]]; then
            pool=$(timeout "$((ssh_t + 5))" ssh -o ConnectTimeout="$ssh_t" "${user}@${fqdn}" "zfs get -H -o value zep:node:${alias}:fs $ds_raw 2>/dev/null | grep -v '^-' | head -n 1")
        fi
    fi
    
    if [[ -z "$pool" || "$pool" == "-" ]]; then
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

resolve_node_filesystem() {
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
    local RS=$'\x1e'
    local props=""
    # Use in-memory cache if populated, otherwise query ZFS directly
    if [[ ${#ZEP_PROP_CACHE[@]} -gt 0 ]]; then
        for key in "${!ZEP_PROP_CACHE[@]}"; do
            [[ "$key" == "${ds}:zep:"* ]] || continue
            local prop="${key#${ds}:}"
            props+="${prop}=${ZEP_PROP_CACHE[$key]}${RS}"
        done
    else
        # Use \x1e (record separator) as delimiter since values can contain ; or spaces
        local raw
        raw=$(zfs get all -H -o property,value "$ds" | grep "^zep:" | awk -F'\t' '{print $1"="$2}')
        while IFS= read -r line; do
            props+="${line}${RS}"
        done <<< "$raw"
    fi
    echo -n "$props" | base64 -w 0
}

apply_repl_props() {
    local ds=$1
    local encoded=$2
    [[ -z "$encoded" ]] && return

    echo -e "${CHAIN_PREFIX}  ${C_DIM}⚙️${C_RESET}  Syncing replication properties for $ds..."
    local decoded
    decoded=$(echo -n "$encoded" | base64 -d 2>/dev/null)
    local rc=$?
    if [[ $rc -ne 0 || -z "$decoded" ]]; then
        echo -e "${CHAIN_PREFIX}    ${C_RED}⚠️  Failed to decode properties${C_RESET}"
        return
    fi
    IFS=$'\x1e' read -ra props <<< "$decoded"
    for p in "${props[@]}"; do
        if [[ -n "$p" ]]; then
            local prop_key="${p%%=*}"
            # Skip ZFS readonly properties
            [[ "$prop_key" =~ ^(guid|used|available|referenced|compressratio|logicalused|usedbysnapshots|usedbydataset|usedbyrefreservation|usedbychildren|creation|written|logicalreferenced|volblocksize|refreservation|refquota|quota)$ ]] && continue
            local current_val=$(get_zfs_prop "$prop_key" "$ds")
            local new_val="${p#*=}"
            if [[ "$current_val" != "$new_val" ]]; then
                 if [[ "$DRY_RUN" == true ]]; then
                    echo -e "${CHAIN_PREFIX}    [DRY RUN] Would update $prop_key -> $new_val"
                 else
                    echo -e "${CHAIN_PREFIX}    Updating $prop_key -> $new_val"
                    zfs set "$p" "$ds" || echo -e "${CHAIN_PREFIX}    ${C_YELLOW}⚠️  WARNING:${C_RESET} Failed to set $p"
                 fi
            fi
        fi
    done
}

seed_cache_from_encoded() {
    local ds=$1
    local encoded=$2
    [[ -z "$encoded" ]] && return
    
    local decoded
    decoded=$(echo -n "$encoded" | base64 -d 2>/dev/null)
    [[ -z "$decoded" ]] && return
    
    IFS=$'\x1e' read -ra props <<< "$decoded"
    for p in "${props[@]}"; do
        [[ -z "$p" ]] && continue
        local k="${p%%=*}"
        local v="${p#*=}"
        ZEP_PROP_CACHE["${ds}:${k}"]="$v"
    done
}

init_colors() {
    # Color detection
    if [[ "$ZEP_BW" != "true" && ( "$ZEP_FORCE_COLOR" == "true" || ( -t 1 && -n "$TERM" && "$TERM" != "dumb" ) ) ]]; then
        C_RED='\e[31m'
        C_GREEN='\e[32m'
        C_YELLOW='\e[33m'
        C_BLUE='\e[34m'
        C_CYAN='\e[36m'
        C_BOLD='\e[1m'
        C_DIM='\e[2m'
        C_RESET='\e[0m'
        export ZEP_COLORS_ENABLED="true"
    else
        C_RED=''
        C_GREEN=''
        C_YELLOW=''
        C_BLUE=''
        C_CYAN=''
        C_BOLD=''
        C_DIM=''
        C_RESET=''
        export ZEP_COLORS_ENABLED="false"
    fi
}
init_colors

zbud_msg() { 
    local msg="${CHAIN_PREFIX}    $*"
    echo -e "$msg" 1>&2
    local alias=${CLI_ALIAS:-$(hostname)}
    local prefix=${REPL_SNAP_PREFIX:-zep_}
    local uid=${REPL_LOG_UID:-$(id -u)}
    local cmd=${REPL_LOG_CMD:-zep}
    local log_file="/tmp/${prefix}${cmd}-${alias}-${uid}.log"
    # Strip ANSI codes, non-ASCII (emojis), and leading space/pipes from the message
    local clean_msg=$(echo -e "$*" | sed 's/\x1b\[[0-9;]*m//g' | perl -CS -pe 's/[^\x20-\x7E]//g' | sed -e 's/^[[:space:]|]*//')
    if [[ ! "$clean_msg" =~ ^(INFO|WARNING|ERROR|AUDIT|REPLICATION|ROTATION): ]]; then
        clean_msg="INFO: $clean_msg"
    fi
    echo -e "$(date '+%Y-%m-%d %H:%M:%S') [$alias] $clean_msg" >> "$log_file" 2>/dev/null || true
}
zbud_warn() { zbud_msg "  ${C_YELLOW}⚠️  WARNING:${C_RESET} $*"; }

indent_output() {
    sed "s/^/${CHAIN_PREFIX}        /"
}

die() {
    local msg="$1"
    local exit_code=${2:-1}
    shift 2 || true
    local detail_flag=""
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --detail) detail_flag="--detail" ;;
        esac
        shift
    done
    zbud_msg "  ${C_RED}❌ ERROR:${C_RESET} $msg"

    if [[ -n "$local_ds" ]]; then
        if type send_smtp_alert >/dev/null 2>&1; then
            if [[ -n "$detail_flag" ]]; then
                send_smtp_alert "critical" --detail "ERROR: $msg"
            else
                send_smtp_alert "critical" "ERROR: $msg"
            fi
        fi
    fi
    if [[ "$CASCADED" != true ]]; then
        if [[ -f "$REPL_HINT_FILE" ]]; then
            # Only print from file if it wasn't already printed (exit code 2 means it was likely printed by zfsbud_core)
            if [[ "$exit_code" -ne 2 ]]; then
                echo ""
                echo -e "$(cat "$REPL_HINT_FILE" | sed 's/|HINT_NL|/\\n/g')"
            fi
            rm -f "$REPL_HINT_FILE"
        elif [[ "$exit_code" -eq 1 ]]; then
            echo ""
            echo -e "${C_BOLD}HINT: If replication failed because there is no common ground:${C_RESET}"
            echo "  - For a new destination: try adding the '--init' flag."
            echo "  - To rebuild an existing broken chain: use '--promote --auto -y' on the Master."
            echo "  - To force a fresh start (DANGER): use '--promote --destroy-chain' on the Master."
        fi
    fi
    exit $exit_code
}

cleanup() {
    [[ -n "$LOCKFILE" ]] && rm -f "$LOCKFILE"
}

check_stuck_job() {
    local alias_val=${CLI_ALIAS:-$(hostname)}
    local prefix=$(get_snap_prefix "$filesystem")
    local lock_name="${prefix}${alias_val}-${filesystem//\//-}-${label}.lock"
    LOCKFILE="/tmp/${lock_name}"
    export LOCKFILE
    
    local timeout_val=$(resolve_proc_timeout "$filesystem")
    
    # Determine if we should wait or fail fast
    local wait_for_lock=false
    if [[ "$CASCADED" == true || "$PROMOTE" == true || "$SUSPEND" == true || "$RESUME" == true || "$MARK_ONLY" == true || -t 0 ]]; then
        wait_for_lock=true
    fi

    local waited=0
    while [[ -f "$LOCKFILE" ]]; do
        local lock_pid=$(cat "$LOCKFILE" 2>/dev/null)
        
        # Self-healing: Check if the PID is actually running
        if [[ -n "$lock_pid" ]]; then
            if ! kill -0 "$lock_pid" 2>/dev/null; then
                zbud_msg "  ⚠️  Stale lock detected (PID $lock_pid is not running). Cleaning up..."
                rm -f "$LOCKFILE"
                continue
            fi
        fi

        if [[ "$wait_for_lock" == true ]]; then
            if [[ $waited -ge $timeout_val ]]; then
                die "ERR: Timeout waiting for lock $LOCKFILE after $timeout_val seconds. Lock held by PID: $lock_pid"
            fi
            echo "${CHAIN_PREFIX}Lock $LOCKFILE held by PID $lock_pid. Waiting... ($waited/${timeout_val}s)"
            sleep 10
            waited=$((waited + 10))
            continue
        fi

        local cur_time=$(date +%s)
        local m_time=$(stat -c %Y "$LOCKFILE" 2>/dev/null || echo "$cur_time")
        local age=$((cur_time - m_time))
        
        if [[ "$age" -gt "$timeout_val" ]]; then
            # Progress Check: Is data actually moving?
            if check_replication_progress "$filesystem"; then
                zbud_msg "  ⏳ Job exceeded timeout ($((age/60)) min) but progress is being made. Skipping alert and letting it continue."
                touch "$LOCKFILE" # Reset age to prevent constant re-checking
                exit 0
            fi

            if type send_smtp_alert >/dev/null 2>&1; then
                send_smtp_alert "critical" "CRITICAL: ZFS replication job for $filesystem ($label) is stuck. Lock file: $LOCKFILE. Age: $((age/60)) min. Timeout: $((timeout_val/60)) min. PID recorded: $lock_pid"
            fi
            die "ERR: Stuck job detected ($age seconds old) at $LOCKFILE. Alert sent."
        else
            zbud_msg "${C_DIM}ℹ️${C_RESET}  Replication already running ($age seconds ago) at $LOCKFILE. PID: $lock_pid. Skipping run."
            exit 0
        fi
    done

    echo "$$" > "$LOCKFILE"
}

# High-performance pipe monitor to track bytes and update progress file
iomon() {
    local lock="$1"
    local interval="$2"
    /usr/local/bin/iomon "$lock" "$interval"
}

check_replication_progress() {
    local ds="$1"
    local cnt_file="${LOCKFILE}.cnt"
    [[ -f "$cnt_file" ]] || return 1
    
    local last_size=$(cat "$cnt_file" 2>/dev/null || echo 0)

    sleep 2
    local current_size=$(cat "$cnt_file" 2>/dev/null || echo 0)

    if [[ "$current_size" -gt "$last_size" ]]; then
        return 0 # Progress made
    fi

    return 1 # Stuck
}


