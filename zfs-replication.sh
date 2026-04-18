#!/bin/bash

# ZFS Replication Manager
# Incorporates zfsbud.sh logic for robust ZFS sending/receiving.
# Credit for zfsbud.sh goes to Pawel Ginalski (https://gbyte.dev / gbytedev)

dir=$(dirname "$0")

# Helper functions
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

# Resolve retention (keep count) for the current node
resolve_retention() {
    local ds=$1
    local lbl=$2
    local fallback=$3
    local role="middle"
    
    [[ $ME_INDEX -eq 0 ]] && role="master"
    [[ $ME_INDEX -eq $((${#nodes[@]} - 1)) ]] && role="sink"
    
    local val=""
    
    # 1. Host-specific: repl:node:<alias>:keep:<label>
    val=$(get_zfs_prop "repl:node:${ME}:keep:${lbl}" "$ds")
    
    # 2. Role-specific: repl:role:<role>:keep:<label>
    [[ -z "$val" ]] && val=$(get_zfs_prop "repl:role:${role}:keep:${lbl}" "$ds")
    
    # 3. Final Fallback
    [[ -z "$val" ]] && val="$fallback"
    
    echo "$val"
}

find_best_donor() {
    local target_node=$1
    local ds_raw=$2
    local target_pool=$(resolve_node_pool "$target_node" "$ds_raw")
    
    # Iterate ALL nodes in chain to find someone who shares a GUID with target
    for (( k=${#nodes[@]}-1; k>=0; k-- )); do
        local donor_alias="${nodes[k]}"
        [[ "$donor_alias" == "$target_node" ]] && continue
        [[ "$donor_alias" == "$ME" ]] && continue
        
        local donor_fqdn=$(resolve_node_fqdn "$donor_alias" "$ds_raw")
        local donor_user=$(resolve_node_user "$donor_alias" "$ds_raw")
        local donor_target="${donor_user}@${donor_fqdn}"

        # Check connectivity to potential donor
        if ! ssh -o ConnectTimeout=3 "$donor_target" "true" 2>/dev/null; then continue; fi
        
        local donor_pool=$(resolve_node_pool "$donor_alias" "$ds_raw")
        
        # Check if donor has snapshots and shares GUID with target
        if ssh "$donor_target" "zfs list -t snap -H -r ${donor_pool}/${ds_raw#*/} >/dev/null 2>&1"; then
            if ssh "$donor_target" "zfs-replication.sh $ds_raw $label 0 --target $target_node --donor >/dev/null 2>&1"; then
                echo "$donor_alias"
                return 0
            fi
        fi
    done
    return 1
}

send_smtp_alert() {
    local msg=$1
    local host=$(get_zfs_prop "repl:smtp_host" "$dataset")
    local port=$(get_zfs_prop "repl:smtp_port" "$dataset")
    local user=$(get_zfs_prop "repl:smtp_user" "$dataset")
    local pass=$(get_zfs_prop "repl:smtp_password" "$dataset")
    local from=$(get_zfs_prop "repl:smtp_from" "$dataset")
    local to=$(get_zfs_prop "repl:smtp_to" "$dataset")
    local proto=$(get_zfs_prop "repl:smtp_protocol" "$dataset")
    
    [[ -z "$host" || -z "$to" ]] && return

    # --- Rate Limiting Logic ---
    local state_dir="/tmp/zfs-repl-alerts"
    mkdir -p "$state_dir"
    # Use a safe filename for the dataset state
    local ds_safe="${dataset//\//-}"
    local state_file="${state_dir}/${ds_safe}.state"
    # Hash the core message (excluding dynamic error details to group similar errors)
    local msg_hash=$(echo -n "$msg" | md5sum | awk '{print $1}')
    
    local last_sent=0
    local supp_count=0
    if [[ -f "$state_file" ]]; then
        local line=$(grep "^$msg_hash " "$state_file")
        if [[ -n "$line" ]]; then
            last_sent=$(echo "$line" | awk '{print $2}')
            supp_count=$(echo "$line" | awk '{print $3}')
        fi
    fi

    local current_time=$(date +%s)
    local threshold=1800 # 30 minutes
    local elapsed=$((current_time - last_sent))

    if [[ $elapsed -lt $threshold && $last_sent -gt 0 ]]; then
        # Suppress and increment counter
        supp_count=$((supp_count + 1))
        # Update state file: remove old line, append new
        touch "$state_file"
        sed -i "/^$msg_hash /d" "$state_file"
        echo "$msg_hash $last_sent $supp_count" >> "$state_file"
        echo "Alert suppressed (Rate Limit: ${elapsed}s < ${threshold}s). Count: $supp_count"
        return
    fi

    # Prepare summary for suppressed alerts
    local rate_limit_notice=""
    if [[ $supp_count -gt 0 ]]; then
        rate_limit_notice="[Note: In the last $((elapsed / 60)) minutes, this specific alert was repeated and suppressed $supp_count times.]"
    fi
    # --- End Rate Limiting Logic ---

    # Include captured error details if they exist
    local detail=""
    if [[ -f "/tmp/zfs-replication.err" ]]; then
        detail=$(cat /tmp/zfs-replication.err)
        rm -f /tmp/zfs-replication.err
    fi

    echo "Sending alert email to $to..."
    curl -s --url "${proto:-smtps}://${host}:${port:-465}" \
         --user "${user}:${pass}" \
         --mail-from "$from" \
         --mail-rcpt "$to" \
         --upload-file - <<EOF
From: $from
To: $to
Subject: ZFS Replication Alert: $dataset on $(hostname)
Date: $(date -R)

$msg
$rate_limit_notice

--- Error Details ---
${detail:-No specific error details captured.}
EOF

    # Update state file with new sent time and reset counter
    touch "$state_file"
    sed -i "/^$msg_hash /d" "$state_file"
    echo "$msg_hash $current_time 0" >> "$state_file"
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
    
    echo "Syncing replication properties for $ds..."
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

# --- START OF ZFSBUD CORE (Adapted from zfsbud.sh) ---

zbud_PATH=/usr/bin:/sbin:/bin
zbud_timestamp_format="%Y-%m-%d-%H%M%S"

zbud_msg() { echo "$*" 1>&2; }
zbud_warn() { zbud_msg "WARNING: $*"; }
zbud_die() { 
    zbud_msg "ERROR: $*"
    if [[ -n "$dataset" ]]; then
        send_smtp_alert "ERROR in ZFSBUD: $*"
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

zfsbud_core() {
  local PATH=$zbud_PATH
  local timestamp=$(date "+$zbud_timestamp_format")
  local log_file="$HOME/zfsbud_internal.log"
  local RATE=20M
  local BUF=64M
  local resume="-s"
  local snapshot_prefix=$(zbud_config_get default_snapshot_prefix)
  
  local create remove_old send initial recursive_send recursive_create recursive_destroy remote_shell verbose log dry_run snapshot_label destination_parent_dataset
  declare -A src_keep_timestamps=() src_kept_timestamps=() dst_keep_timestamps=() dst_kept_timestamps=()
  local source_snapshots=() destination_snapshots=() last_snapshot_common resume_token

  # Parse args for this internal call
  local OPTIND
  while getopts "cs:in e:Rrp:vldL:h" opt; do
    case $opt in
      c) create=1 ;;
      s) send=1; destination_parent_dataset=$OPTARG ;;
      i) initial=1 ;;
      n) unset zbud_resume ;;
      e) remote_shell=$OPTARG ;;
      R) recursive_send="-R"; recursive_create="-r"; recursive_destroy="-r" ;;
      r) remove_old=1 ;;
      p) snapshot_prefix=$OPTARG ;;
      v) verbose="-v" ;;
      l) log=1 ;;
      d) dry_run=1 ;;
      *) return 1 ;;
    esac
  done
  shift $((OPTIND-1))
  local datasets=("$@")

  # Helper inner functions
  dataset_exists() {
    if [ -n "$remote_shell" ]; then
      $remote_shell "zfs list -H -o name" 2>/dev/null | grep -qx "$1" && return 0
    else
      zfs list -H -o name | grep -qx "$1" && return 0
    fi
    return 1
  }

  set_resume_token() {
    ! dataset_exists "$1" && return 0
    local token="-"
    if [ -n "$remote_shell" ]; then
      token=$($remote_shell "zfs get -H -o value receive_resume_token $1" 2>/dev/null)
    else
      token=$(zfs get -H -o value receive_resume_token "$1")
    fi
    [[ $token ]] && [[ $token != "-" ]] && resume_token=$token
  }

  get_local_snapshots() { zfs list -H -o name,guid -t snapshot | grep "$1@"; }
  get_remote_snapshots() { $remote_shell "zfs list -H -o name,guid -t snapshot | grep $1@"; }
  
  set_source_snapshots() {
    # format: dataset@name<tab>guid
    mapfile -t source_snapshots < <(get_local_snapshots "$1")
  }
  
  set_destination_snapshots() {
    if [ -n "$remote_shell" ]; then
      local output
      output=$($remote_shell "zfs list -H -o name,guid -t snapshot | grep $1@" 2>/dev/null)
      local status=$?
      [[ $status -ne 0 && $status -ne 1 ]] && return $status # Connectivity error
      mapfile -t destination_snapshots <<< "$output"
    else
      mapfile -t destination_snapshots < <(get_local_snapshots "$destination_parent_dataset/$1")
    fi
    return 0
  }

  set_common_snapshot() {
    last_snapshot_common=""
    # Iterate through destination snapshots in reverse (newest first)
    for (( i=${#destination_snapshots[@]}-1; i>=0; i-- )); do
      dest_line="${destination_snapshots[$i]}"
      dest_snap=$(echo "$dest_line" | awk '{print $1}')
      dest_guid=$(echo "$dest_line" | awk '{print $2}')
      dest_label="${dest_snap#*@}"
      
      for source_line in "${source_snapshots[@]}"; do
        source_snap=$(echo "$source_line" | awk '{print $1}')
        source_guid=$(echo "$source_line" | awk '{print $2}')
        if [[ "$source_guid" == "$dest_guid" ]]; then
           last_snapshot_common="${source_snap#*@}"
           zbud_msg "Found common snapshot by GUID: $last_snapshot_common (GUID: $source_guid)"
           return 0
        fi
      done
    done
    [ -n "$last_snapshot_common" ] && return 0 || return 1
  }

  send_initial() {
    local latest_line=${source_snapshots[-1]}
    local latest_snapshot_source=$(echo "$latest_line" | awk '{print $1}')
    zbud_msg "Initial source snapshot (latest): $latest_snapshot_source"
    zbud_msg "Sending initial snapshot to destination..."
    # Map any source pool/dataset to destination_pool/dataset
    local ds_name="${dataset#*/}"
    local remote_ds="${destination_parent_dataset}/${ds_name}"
    
    # Identify LOCAL dataset to send from
    local local_ds="$dataset"

    local timeout_val=$(get_zfs_prop "repl:timeout" "$dataset")
    [[ -z "$timeout_val" ]] && timeout_val="3600"

    if [ -z "$dry_run" ]; then
      # FORCE CLEANUP of destination ONLY if --destroy-chain is set
      if [[ "$DESTROY_CHAIN" == true ]]; then
        zbud_msg "DESTROY_CHAIN: Cleaning up $remote_ds for initial send..."
        if [ -n "$remote_shell" ]; then
          $remote_shell "zfs destroy -r $remote_ds 2>/dev/null || true"
        else
          zfs destroy -r "$remote_ds" 2>/dev/null || true
        fi
      fi

      if [ -n "$remote_shell" ]; then
        ! timeout "$timeout_val" bash -c "zfs send -w -R \"$latest_snapshot_source\" 2>>/tmp/zfs-replication.err | mbuffer -q -r \"$RATE\" -m \"$BUF\" 2>>/tmp/zfs-replication.err | zstd 2>>/tmp/zfs-replication.err | $remote_shell \"zstd -d | zfs recv $resume -F -u $remote_ds\" 2>>/tmp/zfs-replication.err" && return 1
      else
        ! timeout "$timeout_val" bash -c "zfs send -w -R \"$latest_snapshot_source\" 2>>/tmp/zfs-replication.err | zfs recv $resume -F -u \"$remote_ds\" 2>>/tmp/zfs-replication.err" && return 1
      fi
    fi
    last_snapshot_common="${latest_snapshot_source#*@}"
  }

  send_incremental() {
    local last_snapshot_source=${source_snapshots[-1]}
    local latest_snapshot_source=$(echo "$last_snapshot_source" | awk '{print $1}')
    if [[ ${latest_snapshot_source#*@} == "$last_snapshot_common" ]]; then
      zbud_msg "Skipping incremental: already up to date."
      return 0
    fi
    zbud_msg "Sending incremental: $last_snapshot_common -> ${latest_snapshot_source#*@}"
    local ds_name="${dataset#*/}"
    local remote_ds="${destination_parent_dataset}/${ds_name}"
    
    # Identify LOCAL dataset to send from
    local local_ds="$dataset"

    local timeout_val=$(get_zfs_prop "repl:timeout" "$dataset")
    [[ -z "$timeout_val" ]] && timeout_val="3600"

    if [ -z "$dry_run" ]; then
      if [ -n "$remote_shell" ]; then
        set -o pipefail
        # We use a subshell on the remote to capture its stderr and print it to stdout so we can catch it locally
        timeout "$timeout_val" bash -c "zfs send -w -p $recursive_send -i \"$local_ds@$last_snapshot_common\" \"$latest_snapshot_source\" 2>>/tmp/zfs-replication.err | mbuffer -q -r \"$RATE\" -m \"$BUF\" 2>>/tmp/zfs-replication.err | zstd 2>>/tmp/zfs-replication.err | $remote_shell \"zstd -d | zfs recv $resume -F -u $remote_ds\" 2>>/tmp/zfs-replication.err"
        local status=$?
        set +o pipefail
        if [[ $status -ne 0 ]]; then
           zbud_msg "Pipeline failed with status $status"
           return 1
        fi
      else
        ! timeout "$timeout_val" bash -c "zfs send -w -p $recursive_send -i \"$local_ds@$last_snapshot_common\" \"$latest_snapshot_source\" 2>>/tmp/zfs-replication.err | zfs recv $resume -F -u \"$remote_ds\" 2>>/tmp/zfs-replication.err" && return 1
      fi
    fi
  }

  # Simplified processing for zfs-replication.sh context
  for dataset in "${datasets[@]}"; do
    ds_name="${dataset#*/}"
    local_ds="$dataset"
    
    zbud_msg "Processing $local_ds -> ${destination_parent_dataset} (Target: ${destination_parent_dataset}/${ds_name})"
    
    set_source_snapshots "$local_ds"
    if ((${#source_snapshots[@]} < 1)); then
       zbud_warn "No snapshots for $local_ds"
       continue
    fi

    local remote_ds="${destination_parent_dataset}/${ds_name}"
    set_resume_token "$remote_ds"
    
    if [ -n "$resume_token" ]; then
       # Resume logic simplified
       if [ -z "$dry_run" ]; then
         if [ -n "$remote_shell" ]; then
           zfs send -w $verbose -t "$resume_token" | mbuffer -q -r "$RATE" -m "$BUF" | zstd | $remote_shell "zstd -d | zfs recv $resume -F -u ${destination_parent_dataset}"
         else
           zfs send -w $verbose -t "$resume_token" | zfs recv $resume -F -u "${destination_parent_dataset}"
         fi
       fi
    fi

    set_destination_snapshots "$ds_name"
    local ds_status=$?
    if [[ $ds_status -ne 0 && $ds_status -ne 1 ]]; then
       zbud_msg "Target node unreachable or dataset listing failed (Status: $ds_status)"
       return $ds_status
    fi

    if ! set_common_snapshot; then
       if [ -n "$initial" ]; then
          send_initial || return 1
       else
          zbud_warn "No common snapshots for $local_ds. Use -i for initial."
          continue
       fi
    else
       # RESOLVE DIVERGENCE: Rollback receiver to the common snapshot
       local remote_ds="${destination_parent_dataset}/${ds_name}"
       zbud_msg "Rolling back $remote_ds to $last_snapshot_common to resolve divergence..."
       if [ -n "$remote_shell" ]; then
         $remote_shell "zfs rollback -r $remote_ds@$last_snapshot_common" || return 1
       else
         zfs rollback -r "$remote_ds@$last_snapshot_common" || return 1
       fi
    fi
    send_incremental || return 1
  done
  return 0
}

# --- END OF ZFSBUD CORE ---

# Main script helpers
die() {
    echo "$@"
    if [[ -n "$dataset" ]]; then
        send_smtp_alert "ERROR: $*"
    fi
    echo "HINT: If replication failed due to divergent snapshots, try recovery options:"
    echo "  --promote --auto [-y]         (Auto-discover latest common snapshot and rollback chain)"
    echo "  --promote --snap <name> [-y]  (Rollback chain to specific snapshot)"
    echo "  --promote --destroy-chain     (DANGER: Destroy downstream datasets and start over)"
    exit 1
}

purge_shipped_snapshots() {
    local ds=$1
    local lbl=$2
    local k_count=$3
    
    echo "Performing shipped-aware rotation for $ds (label: $lbl, keep: $k_count)..."
    
    # Get snapshots matching label, sorted by creation date (newest first)
    mapfile -t snaps < <(zfs list -t snap -H -o name,zfs-send:shipped -S creation -r "$ds" | grep "@.*$lbl")
    
    local count=${#snaps[@]}
    if [[ $count -le $k_count ]]; then
        echo "  ✅ Snapshot count ($count) is within limit ($k_count). Skipping purge."
        return
    fi
    
    # Process snapshots from index k_count (0-indexed)
    for (( i=k_count; i<count; i++ )); do
        local line="${snaps[i]}"
        read -r snap_name shipped_val <<< "$line"
        
        # Check if shipped
        local is_shipped=false
        if [[ "$line" == *"zfs-send:shipped"* ]]; then
            is_shipped=true
        elif [[ -n "$shipped_val" && "$shipped_val" != "-" ]]; then
            is_shipped=true
        fi

        if [[ "$is_shipped" == true ]]; then
            echo "  🗑️  Purging old shipped snapshot: $snap_name"
            zfs destroy "$snap_name"
        else
            echo "  🛡️  KEEPING old snapshot (NOT YET SHIPPED): $snap_name"
        fi
    done
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
            send_smtp_alert "CRITICAL: ZFS replication job for $dataset ($label) is stuck. Lock file age: $((age/60)) min. Timeout: $((timeout_val/60)) min. PID recorded: $lock_pid"
            die "ERR: Stuck job detected ($age seconds old). Alert sent."
        else
            die "ERR: Replication already running ($age seconds ago). PID: $lock_pid"
        fi
    done

    echo "$$" > "$LOCKFILE"
    trap 'rm -f "$LOCKFILE"' EXIT
}

# Params
raw_dataset=$1
label=${2:-"frequently"}
keep_fallback=${3:-"10"}

# Early local dataset resolution
ds_name="${raw_dataset#*/}"
my_hostname=$(hostname)

# Resolve local pool name from node-specific property (e.g., repl:node1=node1-pool)
configured_pool=$(get_zfs_prop "repl:${my_hostname}" "$raw_dataset")
if [[ -n "$configured_pool" ]]; then
    local_ds="${configured_pool}/${ds_name}"
else
    # Fallback to legacy naming
    local_ds="${my_hostname}-pool/${ds_name}"
fi
dataset=$local_ds # Ensure helper functions use the local path

MARK_ONLY=false
initial_send=false
PROMOTE=false
CASCADED=false
SUSPEND=false
RESUME=false
AUTO=false
DESTROY_CHAIN=false
YES=false
PROMOTE_SNAP=""
sync_props_data=""
TARGET_NODE=""
IS_DONOR=false

# Parse additional flags
shift 3
while [[ $# -gt 0 ]]; do
    case "$1" in
        --mark-only) MARK_ONLY=true; shift ;;
        --initial) initial_send=true; shift ;;
        --promote) PROMOTE=true; shift ;;
        --suspend) SUSPEND=true; shift ;;
        --resume) RESUME=true; shift ;;
        --cascaded) CASCADED=true; shift ;;
        --auto) AUTO=true; shift ;;
        --destroy-chain) DESTROY_CHAIN=true; shift ;;
        -y) YES=true; shift ;;
        --snap) PROMOTE_SNAP="$2"; shift 2 ;;
        --sync-props) sync_props_data="$2"; shift 2 ;;
        --target) TARGET_NODE="$2"; shift 2 ;;
        --donor) IS_DONOR=true; shift ;;
        *) shift ;;
    esac
done

# Handle Suspend/Resume logic
if [[ "$SUSPEND" == true || "$RESUME" == true ]]; then
    ACTION="SUSPENDED"
    VAL="true"
    if [[ "$RESUME" == true ]]; then 
        ACTION="RESUMED"
        VAL="false"
    fi

    echo "${ACTION} replication for $raw_dataset..."
    
    # Discovery chain (try local, then try to find master)
    CURRENT_CHAIN=$(get_zfs_prop "repl:chain" "$local_ds")
    if [[ -z "$CURRENT_CHAIN" ]]; then
        # If not on local, it might be an un-initialized node. 
        # For simplicity in this script, we expect it to be set.
        die "ERR: Cannot ${ACTION}, no existing repl:chain found on $local_ds. Please run from a configured node."
    fi

    IFS=',' read -r -a nodes <<< "$CURRENT_CHAIN"
    for n in "${nodes[@]}"; do
        local n_fqdn=$(resolve_node_fqdn "$n" "$local_ds")
        local n_user=$(resolve_node_user "$n" "$local_ds")
        local n_pool=$(resolve_node_pool "$n" "$local_ds")
        
        echo "  Setting repl:suspend=$VAL on $n ($n_fqdn)..."
        ssh "${n_user}@${n_fqdn}" "zfs set repl:suspend=$VAL ${n_pool}/${ds_name}" || echo "  Warning: Failed to set property on $n"
    done
    
    send_smtp_alert "NOTICE: ZFS Replication has been ${ACTION} for dataset $raw_dataset on $(hostname). Master node: ${nodes[0]}. New state: repl:suspend=$VAL"
    exit 0
fi

# Handle Promotion logic
# Handle Promotion logic
if [[ "$PROMOTE" == true ]]; then
    echo "Promoting $my_hostname to Master..."
    
    # 1. Update Chain Order
    CURRENT_CHAIN=$(get_zfs_prop "repl:chain" "$local_ds")
    if [[ -z "$CURRENT_CHAIN" ]]; then die "ERR: Cannot promote, no existing repl:chain found on $local_ds"; fi

    IFS=',' read -r -a nodes <<< "$CURRENT_CHAIN"
    NEW_NODES=("$my_hostname")
    for n in "${nodes[@]}"; do
        if [[ "$n" != "$my_hostname" ]]; then
            NEW_NODES+=("$n")
        fi
    done
    NEW_CHAIN=$(IFS=','; echo "${NEW_NODES[*]}")
    
    if [[ "$CURRENT_CHAIN" != "$NEW_CHAIN" ]]; then
        echo "  Updating chain: $CURRENT_CHAIN -> $NEW_CHAIN"
        zfs set repl:chain="$NEW_CHAIN" "$local_ds"
        send_smtp_alert "NOTICE: Node $my_hostname has been PROMOTED to Master for dataset $raw_dataset. New chain: $NEW_CHAIN"
        # RE-FETCH identity for the current run
        REPL_CHAIN="$NEW_CHAIN"
        IS_MASTER=true
        ME_INDEX=0
    else
        echo "  $my_hostname is already Master in local config."
    fi
    # 2. Recovery / Consistency Check
    if [[ "$AUTO" == true || -n "$PROMOTE_SNAP" || "$DESTROY_CHAIN" == true ]]; then
        TARGET_SNAP=""
        if [[ -n "$PROMOTE_SNAP" ]]; then
            TARGET_SNAP="$PROMOTE_SNAP"
            echo "Checking if snapshot $TARGET_SNAP exists on all nodes and has consistent GUID..."
            
            # Gather GUID for this snap from all nodes
            declare -A snap_guids
            for n in "${NEW_NODES[@]}"; do
                echo "  Querying $n for GUID of $TARGET_SNAP..."
                local local_pool=$(resolve_node_pool "$n" "$raw_dataset")
                local local_fqdn=$(resolve_node_fqdn "$n" "$raw_dataset")
                local local_user=$(resolve_node_user "$n" "$raw_dataset")
                g=$(ssh "${local_user}@${local_fqdn}" "zfs get -H -o value guid ${local_pool}/${ds_name}@$TARGET_SNAP" 2>/dev/null)
                if [[ -z "$g" || "$g" == "-" ]]; then
                    die "ERR: Snapshot @$TARGET_SNAP not found on $n"
                fi
                snap_guids["$n"]=$g
            done
            
            # Verify consistency
            f_node="${NEW_NODES[0]}"
            ref_guid=${snap_guids[$f_node]}
            for n in "${NEW_NODES[@]}"; do
                if [[ "${snap_guids[$n]}" != "$ref_guid" ]]; then
                    die "ERR: Snapshot @$TARGET_SNAP exists on all nodes but GUIDs mismatch! ($f_node: $ref_guid vs $n: ${snap_guids[$n]}). The chain has diverged."
                fi
            done
            echo "Snapshot @$TARGET_SNAP is consistent across all nodes (GUID: $ref_guid)."
        else
            echo "Auto-discovering latest common snapshot across the chain (using GUIDs)..."
            # Gather snapshots (name and guid) from all nodes
            # We'll use a temporary file to store the common candidates
            tmp_common="/tmp/zfs-common-snaps.$$"
            f_node_flag=true
            
            for n in "${NEW_NODES[@]}"; do
                echo "  Querying $n..."
                local local_pool=$(resolve_node_pool "$n" "$raw_dataset")
                local local_fqdn=$(resolve_node_fqdn "$n" "$raw_dataset")
                local local_user=$(resolve_node_user "$n" "$raw_dataset")
                local node_target="${local_user}@${local_fqdn}"

                if [[ "$f_node_flag" == true ]]; then
                    ssh "$node_target" "zfs list -t snap -H -o name,guid -r ${local_pool}/${ds_name}" 2>/dev/null | awk '{print $1" "$2}' | cut -d'@' -f2 > "$tmp_common"
                    f_node_flag=false
                else
                    node_tmp="/tmp/zfs-node-snaps.$$"
                    ssh "$node_target" "zfs list -t snap -H -o name,guid -r ${local_pool}/${ds_name}" 2>/dev/null | awk '{print $1" "$2}' | cut -d'@' -f2 > "$node_tmp"
                    # Intersect current common with this node's snaps (match both name and guid)
                    grep -Fxf "$tmp_common" "$node_tmp" > "${tmp_common}.new"
                    mv "${tmp_common}.new" "$tmp_common"
                    rm -f "$node_tmp"
                fi
                [[ ! -s "$tmp_common" ]] && break
            done

            if [[ -s "$tmp_common" ]]; then
                # Get the latest one (bottom of the file, assuming zfs list order)
                latest_line=$(tail -n 1 "$tmp_common")
                TARGET_SNAP=$(echo "$latest_line" | awk '{print $1}')
                target_guid=$(echo "$latest_line" | awk '{print $2}')
                echo "Found latest common snapshot: $TARGET_SNAP (GUID: $target_guid)"
            fi
            rm -f "$tmp_common"
        fi

        # SAFETY CHECK for --destroy-chain
        if [[ "$DESTROY_CHAIN" == true && -n "$TARGET_SNAP" ]]; then
            echo "ABORT: Common snapshot @$TARGET_SNAP found!"
            echo "  Destruction is NOT necessary. Please use --promote --auto instead."
            exit 1
        fi

        # If we reach here and it was only --destroy-chain with NO common snap, we continue to replication 
        # (send_initial will handle the actual destruction if DESTROY_CHAIN is true)
        
        if [[ "$AUTO" == true || -n "$PROMOTE_SNAP" ]]; then
            if [[ -z "$TARGET_SNAP" ]]; then
                die "ERR: Could not find a common snapshot across all nodes. Use --destroy-chain if you want to start fresh."
            fi

            # 3. Confirmation
            if [[ "$YES" != true ]]; then
                echo -n "Are you sure you want to rollback ALL nodes in the chain to @$TARGET_SNAP? (y/N): "
                read -r resp
                if [[ "$resp" != "y" ]]; then die "Aborted by user."; fi
            fi

            # 4. Execute Rollbacks
            for n in "${NEW_NODES[@]}"; do
                echo "Rolling back $n to $TARGET_SNAP..."
                local local_pool=$(resolve_node_pool "$n" "$raw_dataset")
                local local_fqdn=$(resolve_node_fqdn "$n" "$raw_dataset")
                local local_user=$(resolve_node_user "$n" "$raw_dataset")
                ssh "${local_user}@${local_fqdn}" "zfs rollback -r ${local_pool}/${ds_name}@$TARGET_SNAP" || die "ERR: Rollback failed on $n"
            done
            echo "Chain successfully consistent at @$TARGET_SNAP"
        fi
    fi
fi 
# Apply propagated properties if provided (if not promoting)
if [[ -n "$sync_props_data" && "$PROMOTE" != true ]]; then
    # Check if dataset exists before applying (it might not on first --initial run)
    if zfs list "$local_ds" >/dev/null 2>&1; then
        apply_repl_props "$local_ds" "$sync_props_data"
    else
        echo "INFO: Dataset $local_ds not found, skipping property sync (likely initial run)."
    fi
fi

# Identity & Configuration Discovery
REPL_CHAIN=$(get_zfs_prop "repl:chain" "$local_ds")
REPL_USER=$(get_zfs_prop "repl:user" "$local_ds")
[[ -z "$REPL_USER" ]] && REPL_USER="root"

echo "Start: $(date); Dataset: $local_ds; Label: $label"

[[ -n "$raw_dataset" ]] || die "dataset not specified"
ME=$(hostname)
NEXT_HOP=""
IS_MASTER=false
ME_INDEX=-1
RESOLVED_KEEP=$keep_fallback

if [[ -n "$REPL_CHAIN" ]]; then
    IFS=',' read -r -a nodes <<< "$REPL_CHAIN"
    NODES_REMAINING=()
    for i in "${!nodes[@]}"; do
        if [[ "${nodes[i]}" == "$ME" ]]; then
            ME_INDEX=$i
            if [[ $i -eq 0 ]]; then IS_MASTER=true; fi
            # Capture all nodes AFTER the current one
            for (( j=i+1; j<${#nodes[@]}; j++ )); do
                NODES_REMAINING+=("${nodes[j]}")
            done
            if (( ${#NODES_REMAINING[@]} > 0 )); then
                NEXT_HOP="${REPL_USER}@${NODES_REMAINING[0]}"
            fi
            break
        fi
    done
    [[ $ME_INDEX -eq -1 ]] && die "ERR: Host $ME is not part of the replication chain for $local_ds"
    
    # Resolve Graduated Retention
    RESOLVED_KEEP=$(resolve_retention "$local_ds" "$label" "$keep_fallback")
    echo "INFO: Using dynamic retention for $label: $RESOLVED_KEEP (Role: $( [[ $ME_INDEX -eq 0 ]] && echo "master" || ( [[ $ME_INDEX -eq $((${#nodes[@]}-1)) ]] && echo "sink" || echo "middle" ) ))"
fi

if [[ -n "$TARGET_NODE" ]]; then
    NODES_REMAINING=("$TARGET_NODE")
    IS_MASTER=true # Force initiation for manual/delegated targets
fi

# Cron Safety: Only the master node initiates replication.
# Downstream nodes only run if explicitly triggered via --cascaded, --promote, --mark-only, --target, or --donor.
if [[ "$IS_MASTER" == false && "$CASCADED" == false && "$PROMOTE" == false && "$MARK_ONLY" == false && -z "$TARGET_NODE" && "$IS_DONOR" == false ]]; then
    echo "INFO: Node $ME is not Master. Skipping initiation (Cron safety)."
    exit 0
fi

# Suspend check (only affects Master initiation)
if [[ "$IS_MASTER" == true && "$CASCADED" == false && "$PROMOTE" == false && "$MARK_ONLY" == false && -z "$TARGET_NODE" ]]; then
    SUSPEND_STATE=$(get_zfs_prop "repl:suspend" "$local_ds")
    if [[ "$SUSPEND_STATE" == "true" ]]; then
        echo "INFO: Replication is SUSPENDED (repl:suspend=true). Skipping run."
        exit 0
    fi
fi

if [[ "$MARK_ONLY" == true ]]; then
    if [[ "$IS_MASTER" == true ]]; then
        purge_shipped_snapshots "$local_ds" "$label" "$RESOLVED_KEEP"
    fi
    exit 0
fi

# Safety check
check_stuck_job

# 1. Snapshot creation (Master only)
if [[ "$IS_MASTER" == true && "$CASCADED" == false && "$PROMOTE" == false && -z "$TARGET_NODE" && "$IS_DONOR" == false ]]; then
    k_flag=$(cat /var/run/keep-$label.txt 2> /dev/null)
    [[ -z "$k_flag" ]] && k_flag=999
    
    echo "Creating snapshot for $local_ds (label: $label)..."
    /usr/sbin/zfs-auto-snapshot --syslog --label=$label --keep=$k_flag "$local_ds"
    [[ $? -eq 0 ]] || die "ERR: snapshot creation failed"
else
    if [[ "$IS_DONOR" == true ]]; then
        echo "INFO: Node $ME is acting as Donor. Skipping snapshot creation."
    elif [[ -n "$TARGET_NODE" ]]; then
        echo "INFO: Point-to-point transfer to $TARGET_NODE. Skipping snapshot creation."
    else
        echo "INFO: Not a master host ($ME), skipping snapshot creation."
    fi
fi

# Identify local "latest" snapshot for verification
LATEST_SNAP=$(zfs list -t snap -o name -H -S creation -r "$local_ds" | grep "@.*$label" | head -n 1 | cut -d'@' -f2)

# 2. Replication & Audit
REPLICATION_SUCCESS=false
for hop_node in "${NODES_REMAINING[@]}"; do
    local hop_fqdn=$(resolve_node_fqdn "$hop_node" "$raw_dataset")
    local hop_user=$(resolve_node_user "$hop_node" "$raw_dataset")
    HOP_TARGET="${hop_user}@${hop_fqdn}"
    
    echo "Checking connectivity to $hop_node ($hop_fqdn)..."
    NEXT_HOP_POOL=$(resolve_node_pool "$hop_node" "$raw_dataset")
    if [[ $? -eq 255 ]]; then
        echo "ERROR: Node $hop_node is unreachable (Pre-flight). Skipping..."
        continue
    fi
    
    echo "Attempting replication: $local_ds -> $HOP_TARGET (Pool: $NEXT_HOP_POOL)..."
    zfsbud_opts=""
    if [[ "$initial_send" == true ]]; then zfsbud_opts="-i"; fi
    
    # 2.1 Try local-to-remote first
    TRANSFER_DONE=false
    if zfsbud_core $zfsbud_opts -s "$NEXT_HOP_POOL" -e "ssh $HOP_TARGET" -v "$local_ds"; then
        echo "Replication to $HOP_TARGET successful."
        TRANSFER_DONE=true
    else
        # 2.2 Local failed. Can we find a peer donor?
        echo "WARNING: Local replication to $hop_node failed. Searching chain for a better donor..."
        DONOR_NODE=$(find_best_donor "$hop_node" "$raw_dataset")
        if [[ -n "$DONOR_NODE" ]]; then
            local donor_fqdn=$(resolve_node_fqdn "$DONOR_NODE" "$raw_dataset")
            local donor_user=$(resolve_node_user "$DONOR_NODE" "$raw_dataset")
            local donor_target="${donor_user}@${donor_fqdn}"

            echo "SUCCESS: Found donor peer '$DONOR_NODE' ($donor_fqdn). Delegating healing of '$hop_node'..."
            # Run the script on the donor to push to the target
            if ssh "$donor_target" "zfs-replication.sh $raw_dataset $label $keep_fallback --target $hop_node --donor"; then
                echo "Delegated replication from $DONOR_NODE to $hop_node successful."
                TRANSFER_DONE=true
            else
                echo "ERROR: Delegated replication from $DONOR_NODE failed."
            fi
        else
            echo "ERROR: No suitable donor found for $hop_node."
        fi
    fi

    if [[ "$TRANSFER_DONE" == true ]]; then
        REPLICATION_SUCCESS=true
        
        # PROPAGATE & VERIFY
        echo "Cascading: triggering downstream chain for $local_ds on $HOP_TARGET"
        casc_opts=""
        if [[ "$initial_send" == true ]]; then casc_opts="--initial"; fi
        PROPS_ARG=$(get_repl_props_encoded "$local_ds")
        
        # We run the cascaded script.
        DOWNSTREAM_OUT=$(ssh "$HOP_TARGET" "zfs-replication.sh $raw_dataset $label $keep_fallback $casc_opts --sync-props $PROPS_ARG --cascaded" 2>&1)
        SSH_STATUS=$?
        
        # Bubble up logs
        echo "$DOWNSTREAM_OUT" | grep -v "^SENT_LIST:"
        
        if [[ $SSH_STATUS -eq 0 ]]; then
            ARRIVED_LIST=$(echo "$DOWNSTREAM_OUT" | grep "^SENT_LIST:" | cut -d':' -f2)
            
            if [[ -n "$LATEST_SNAP" && ",$ARRIVED_LIST," == *",$LATEST_SNAP,"* ]]; then
                echo "VERIFICATION SUCCESS: Snapshot $LATEST_SNAP confirmed reaching a sink node."
                REPLICATION_SUCCESS=true # Redundant but clear
            else
                echo "WARNING: Verification FAILED. Snapshot $LATEST_SNAP not confirmed at end of chain, but transfer to $hop_node succeeded."
            fi
        else
            echo "WARNING: Downstream cascade from $hop_node failed (Code: $SSH_STATUS)."
        fi
        
        # If we reached this point, the local transfer to THIS hop was successful.
        # We can stop trying further hops from THIS node.
        break
    else
        echo "ERROR: Replication to $hop_node failed. Skipping to next available node in chain..."
    fi
done

if [[ "$REPLICATION_SUCCESS" == true ]]; then
    # HOUSEKEEPING (Only if at least one remote hop succeeded)
    echo "Marking local snapshots ($local_ds) as shipped..."
    zfs list -t snap -o name -H -r "$local_ds" | grep "@.*$label" | \
    while read s; do
        zfs set zfs-send:shipped=true "$s"
    done
    purge_shipped_snapshots "$local_ds" "$label" "$RESOLVED_KEEP"
    
    # Report our success list for upstream verification
    MY_LIST=$(zfs list -t snap -o name -H -S creation -r "$local_ds" | grep "@.*$label" | cut -d'@' -f2 | xargs | tr ' ' ',')
    echo "SENT_LIST:$MY_LIST"
elif [[ ${#NODES_REMAINING[@]} -gt 0 ]]; then
    echo 9999 > /var/run/keep-$label.txt
    die "ERR: All downstream replication attempts failed for chain: ${NODES_REMAINING[*]}"
else
    # End of chain logic (no remaining nodes)
    if [[ "$IS_DONOR" == true ]]; then
        echo "INFO: Donor run complete."
    else
        echo "INFO: End of chain ($ME). Reporting state."
        /usr/sbin/zfs-auto-snapshot --syslog --label=$label --keep=$RESOLVED_KEEP "$local_ds"
        
        # SINK HOUSEKEEPING
        echo "Sink node marking snapshots ($local_ds) as shipped..."
        zfs list -t snap -o name -H -r "$local_ds" | grep "@.*$label" | \
        while read s; do
            zfs set zfs-send:shipped=true "$s"
        done
        purge_shipped_snapshots "$local_ds" "$label" "$RESOLVED_KEEP"
    fi

    SINK_LIST=$(zfs list -t snap -o name -H -S creation -r "$local_ds" | grep "@.*$label" | cut -d'@' -f2 | xargs | tr ' ' ',')
    echo "SENT_LIST:$SINK_LIST"
fi

echo "Done: $(date)"
exit 0
