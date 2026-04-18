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
    local my_alias=$(hostname)
    
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
    
    # 2. Check local properties if we are on that node
    if [[ -z "$pool" && "$alias" == "$my_alias" ]]; then
        pool=$(get_zfs_prop "repl:node:${alias}:fs" "${alias}-pool/${ds_raw#*/}")
    fi
    
    if [[ -z "$pool" ]]; then
        if [[ "$alias" == "$my_alias" ]]; then
            if zfs list pool >/dev/null 2>&1; then pool="pool"; fi
        else
            local fqdn=$(resolve_node_fqdn "$alias" "$ds_raw")
            local user=$(resolve_node_user "$alias" "$ds_raw")
            if ssh "${user}@${fqdn}" "zfs list pool >/dev/null 2>&1"; then pool="pool"; fi
        fi
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
    val=$(get_zfs_prop "repl:node:${ME}:keep:${lbl}" "$ds")
    [[ -z "$val" ]] && val=$(get_zfs_prop "repl:role:${role}:keep:${lbl}" "$ds")
    [[ -z "$val" ]] && val="$fallback"
    echo "$val"
}

find_best_donor() {
    [[ "$IS_DONOR" == true ]] && return 1 # Don't search if we are already a donor

    local target_node=$1
    local ds_raw=$2
    for (( k=${#nodes[@]}-1; k>=0; k-- )); do
        local donor_alias="${nodes[k]}"
        [[ "$donor_alias" == "$target_node" ]] && continue
        [[ "$donor_alias" == "$ME" ]] && continue
        
        local donor_fqdn=$(resolve_node_fqdn "$donor_alias" "$ds_raw")
        local donor_user=$(resolve_node_user "$donor_alias" "$ds_raw")
        local donor_target="${donor_user}@${donor_fqdn}"

        if ! ssh -o ConnectTimeout=3 "$donor_target" "true" 2>/dev/null; then continue; fi
        
        local donor_pool=$(resolve_node_pool "$donor_alias" "$ds_raw")
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

    local state_dir="/tmp/zfs-repl-alerts"
    mkdir -p "$state_dir"
    local ds_safe="${dataset//\//-}"
    local state_file="${state_dir}/${ds_safe}.state"
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
    local threshold=1800 
    local elapsed=$((current_time - last_sent))

    if [[ $elapsed -lt $threshold && $last_sent -gt 0 ]]; then
        supp_count=$((supp_count + 1))
        touch "$state_file"
        sed -i "/^$msg_hash /d" "$state_file"
        echo "$msg_hash $last_sent $supp_count" >> "$state_file"
        echo "Alert suppressed (Rate Limit: ${elapsed}s < ${threshold}s). Count: $supp_count"
        return
    fi

    local rate_limit_notice=""
    if [[ $supp_count -gt 0 ]]; then
        rate_limit_notice="[Note: In the last $((elapsed / 60)) minutes, this specific alert was repeated and suppressed $supp_count times.]"
    fi

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

    touch "$state_file"
    sed -i "/^$msg_hash /d" "$state_file"
    echo "$msg_hash $current_time 0" >> "$state_file"
}

get_repl_props_encoded() {
    local ds=$1
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

# --- START OF ZFSBUD CORE ---

zbud_PATH=/usr/bin:/sbin:/bin
zbud_timestamp_format="%Y-%m-%d-%H%M%S"
zbud_msg() { echo "$*" 1>&2; }
zbud_warn() { zbud_msg "WARNING: $*"; }
zbud_die() { 
    zbud_msg "ERROR: $*"
    if [[ -n "$dataset" ]]; then send_smtp_alert "ERROR in ZFSBUD: $*"; fi
    exit 1
}

zbud_config_read_file() { (grep -E "^${2}=" -m 1 "${1}" 2>/dev/null || echo "VAR=__UNDEFINED__") | head -n 1 | cut -d '=' -f 2-; }
zbud_config_get() {
  local working_dir="$(dirname "$(readlink -f "$0")")"
  local val="$(zbud_config_read_file $working_dir/zfsbud.conf "${1}")";
  if [ "${val}" = "__UNDEFINED__" ]; then
    val="$(zbud_config_read_file $working_dir/default.zfsbud.conf "${1}")";
    if [ "${val}" = "__UNDEFINED__" ]; then
      case "$1" in
        default_snapshot_prefix) echo "zfsbud_" ;;
        *) echo "0" ;;
      esac
      return
    fi
  fi
  printf -- "%s" "${val}";
}

zfsbud_core() {
  local PATH=$zbud_PATH
  local timestamp=$(date "+$zbud_timestamp_format")
  local RATE=20M
  local BUF=64M
  local resume="-s"
  local create remove_old send initial recursive_send recursive_create recursive_destroy remote_shell verbose log dry_run snapshot_label destination_parent_dataset
  local source_snapshots=() destination_snapshots=() last_snapshot_common resume_token
  local OPTIND
  while getopts "cs:in e:Rrp:vldL:h" opt; do
    case $opt in
      c) create=1 ;;
      s) send=1; destination_parent_dataset=$OPTARG ;;
      i) initial=1 ;;
      e) remote_shell=$OPTARG ;;
      R) recursive_send="-R"; recursive_create="-r"; recursive_destroy="-r" ;;
      r) remove_old=1 ;;
      v) verbose="-v" ;;
      d) dry_run=1 ;;
      *) return 1 ;;
    esac
  done
  shift $((OPTIND-1))
  local datasets=("$@")

  dataset_exists() {
    if [ -n "$remote_shell" ]; then $remote_shell "zfs list -H -o name" 2>/dev/null | grep -qx "$1" && return 0
    else zfs list -H -o name | grep -qx "$1" && return 0; fi
    return 1
  }

  set_resume_token() {
    ! dataset_exists "$1" && return 0
    local token="-"
    if [ -n "$remote_shell" ]; then token=$($remote_shell "zfs get -H -o value receive_resume_token $1" 2>/dev/null)
    else token=$(zfs get -H -o value receive_resume_token "$1"); fi
    [[ $token ]] && [[ $token != "-" ]] && resume_token=$token
  }

  get_local_snapshots() { zfs list -H -o name,guid -t snapshot | grep "$1@"; }
  get_remote_snapshots() { $remote_shell "zfs list -H -o name,guid -t snapshot | grep $1@"; }
  set_source_snapshots() { mapfile -t source_snapshots < <(get_local_snapshots "$1"); }
  set_destination_snapshots() {
    destination_snapshots=()
    local ds_name="${dataset#*/}"
    local remote_ds="${destination_parent_dataset}/${ds_name}"

    if [ -n "$remote_shell" ]; then
      local output
      output=$($remote_shell "zfs list -H -o name,guid -t snapshot | grep \"^${remote_ds}@\"" 2>/dev/null)
      # zbud_msg "DEBUG: raw_output_count=$(echo "$output" | grep "@" | wc -l)"
      local status=$?
      [[ $status -ne 0 && $status -ne 1 ]] && return $status
      mapfile -t destination_snapshots < <(echo "$output" | grep "@")
    else
      mapfile -t destination_snapshots < <(zfs list -H -o name,guid -t snapshot | grep "^${destination_parent_dataset}/${ds_name}@")
    fi
    return 0
  }
  set_common_snapshot() {
    last_snapshot_common=""
    zbud_msg "DEBUG: source_snapshots_count=${#source_snapshots[@]}"
    zbud_msg "DEBUG: destination_snapshots_count=${#destination_snapshots[@]}"
    [[ ${#destination_snapshots[@]} -eq 0 || ${#source_snapshots[@]} -eq 0 ]] && return 1

    for (( i=${#destination_snapshots[@]}-1; i>=0; i-- )); do
      dest_line="${destination_snapshots[$i]}"
      dest_snap=$(echo "$dest_line" | awk '{print $1}')
      dest_guid=$(echo "$dest_line" | awk '{print $2}')
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
    return 1
  }

  check_divergence() {
    local remote_ds="${destination_parent_dataset}/${ds_name}"
    zbud_msg "Checking for divergence on $remote_ds since @$last_snapshot_common..."
    local diff_out=""
    if [ -n "$remote_shell" ]; then diff_out=$($remote_shell "zfs diff $remote_ds@$last_snapshot_common $remote_ds" 2>/dev/null | head -n 20)
    else diff_out=$(zfs diff "$remote_ds@$last_snapshot_common" "$remote_ds" 2>/dev/null | head -n 20); fi
    if [[ -n "$diff_out" ]]; then
      zbud_msg "WARNING: Divergence detected on destination!"
      echo "DIVERGENCE DETECTED on $remote_ds since @$last_snapshot_common:" >> /tmp/zfs-replication.err
      echo "$diff_out" >> /tmp/zfs-replication.err
      return 0
    fi
    return 1
  }

  send_initial() {
    local latest_line=${source_snapshots[-1]}
    local latest_snapshot_source=$(echo "$latest_line" | awk '{print $1}')
    local ds_name="${dataset#*/}"
    local remote_ds="${destination_parent_dataset}/${ds_name}"
    local local_ds="$dataset"
    local timeout_val=$(get_zfs_prop "repl:timeout" "$dataset")
    [[ -z "$timeout_val" ]] && timeout_val="3600"
    if [ -z "$dry_run" ]; then
      if [[ "$DESTROY_CHAIN" == true ]]; then
        if [ -n "$remote_shell" ]; then $remote_shell "zfs destroy -r $remote_ds 2>/dev/null || true"
        else zfs destroy -r "$remote_ds" 2>/dev/null || true; fi
      fi
      if [ -n "$remote_shell" ]; then ! timeout "$timeout_val" bash -c "zfs send -w -R \"$latest_snapshot_source\" 2>>/tmp/zfs-replication.err | mbuffer -q -r \"$RATE\" -m \"$BUF\" 2>>/tmp/zfs-replication.err | zstd 2>>/tmp/zfs-replication.err | $remote_shell \"zstd -d | zfs recv $resume -F -u $remote_ds\" 2>>/tmp/zfs-replication.err" && return 1
      else ! timeout "$timeout_val" bash -c "zfs send -w -R \"$latest_snapshot_source\" 2>>/tmp/zfs-replication.err | zfs recv $resume -F -u \"$remote_ds\" 2>>/tmp/zfs-replication.err" && return 1; fi
    fi
    last_snapshot_common="${latest_snapshot_source#*@}"
  }

  send_incremental() {
    local latest_line=${source_snapshots[-1]}
    local last_snapshot_source=$(echo "$latest_line" | awk '{print $1}')
    if [[ ${last_snapshot_source#*@} == "$last_snapshot_common" ]]; then zbud_msg "Skipping: up to date."; return 0; fi
    local ds_name="${dataset#*/}"
    local remote_ds="${destination_parent_dataset}/${ds_name}"
    local local_ds="$dataset"
    local timeout_val=$(get_zfs_prop "repl:timeout" "$dataset")
    [[ -z "$timeout_val" ]] && timeout_val="3600"
    if [ -z "$dry_run" ]; then
      if [ -n "$remote_shell" ]; then
        set -o pipefail
        timeout "$timeout_val" bash -c "zfs send -w -p $recursive_send -i \"$local_ds@$last_snapshot_common\" \"$last_snapshot_source\" 2>>/tmp/zfs-replication.err | mbuffer -q -r \"$RATE\" -m \"$BUF\" 2>>/tmp/zfs-replication.err | zstd 2>>/tmp/zfs-replication.err | $remote_shell \"zstd -d | zfs recv $resume -F -u $remote_ds\" 2>>/tmp/zfs-replication.err"
        local status=$?
        set +o pipefail
        [[ $status -ne 0 ]] && return 1
      else ! timeout "$timeout_val" bash -c "zfs send -w -p $recursive_send -i \"$local_ds@$last_snapshot_common\" \"$last_snapshot_source\" 2>>/tmp/zfs-replication.err | zfs recv $resume -F -u \"$remote_ds\" 2>>/tmp/zfs-replication.err" && return 1; fi
    fi
  }

  for dataset in "${datasets[@]}"; do
    ds_name="${dataset#*/}"
    local_ds="$dataset"
    set_source_snapshots "$local_ds"
    [[ ((${#source_snapshots[@]} < 1)) ]] && continue
    local remote_ds="${destination_parent_dataset}/${ds_name}"
    set_resume_token "$remote_ds"
    if [ -n "$resume_token" ] && [ -z "$dry_run" ]; then
      if [ -n "$remote_shell" ]; then zfs send -w $verbose -t "$resume_token" | mbuffer -q -r "$RATE" -m "$BUF" | zstd | $remote_shell "zstd -d | zfs recv $resume -F -u ${destination_parent_dataset}"
      else zfs send -w $verbose -t "$resume_token" | zfs recv $resume -F -u "${destination_parent_dataset}"; fi
    fi
    set_destination_snapshots "$ds_name"
    local ds_status=$?
    [[ $ds_status -ne 0 && $ds_status -ne 1 ]] && return $ds_status
    if ! set_common_snapshot; then
       if [[ "$initial" == "1" ]]; then 
          send_initial || return 1
       else 
          zbud_warn "No common snapshots for $local_ds. Use -i for initial."
          continue
       fi
    else
       # Check for local changes before destroying them with rollback/force
       check_divergence

       # RESOLVE DIVERGENCE: Rollback receiver to the common snapshot
       local remote_ds="${destination_parent_dataset}/${ds_name}"
       if [ -n "$remote_shell" ]; then $remote_shell "zfs rollback -r $remote_ds@$last_snapshot_common" || return 1
       else zfs rollback -r "$remote_ds@$last_snapshot_common" || return 1; fi
       
       send_incremental || return 1
    fi
  done
  return 0
}

# Main script helpers
die() {
    echo "$@"
    if [[ -n "$dataset" ]]; then send_smtp_alert "ERROR: $*"; fi
    exit 1
}

purge_shipped_snapshots() {
    local ds=$1
    local lbl=$2
    local k_count=$3
    mapfile -t snaps < <(zfs list -t snap -H -o name,zfs-send:shipped -S creation -r "$ds" | grep "@.*$lbl")
    local count=${#snaps[@]}
    [[ $count -le $k_count ]] && return
    for (( i=k_count; i<count; i++ )); do
        local line="${snaps[i]}"
        read -r snap_name shipped_val <<< "$line"
        if [[ "$line" == *"zfs-send:shipped"* || ( -n "$shipped_val" && "$shipped_val" != "-" ) ]]; then
            echo "  🗑️  Purging old shipped snapshot: $snap_name"
            zfs destroy "$snap_name"
        fi
    done
}

# check_stuck_job() {
    # Locking is ONLY for Master-initiated scheduled runs.
    # Other modes (Cascaded, Promote, Donor, Target-specific) bypass to avoid deadlocks.
    if [[ "$CASCADED" == true || "$PROMOTE" == true || "$IS_DONOR" == true || -n "$TARGET_NODE" ]]; then
        return 0
    fi

    local lock_name="${dataset//\//-}-${label}.lock"
    LOCKFILE="/tmp/${lock_name}"
    local timeout_val=$(get_zfs_prop "repl:timeout" "$dataset")
    [[ -z "$timeout_val" ]] && timeout_val="3600"
    
    # For manual terminal runs, we wait; for cron, we fail-fast or check for stuck jobs.
    local wait_for_lock=false
    [[ -t 0 ]] && wait_for_lock=true
    
    local waited=0
    while [[ -f "$LOCKFILE" ]]; do
        local lock_pid=$(cat "$LOCKFILE" 2>/dev/null)
        if [[ "$wait_for_lock" == true ]]; then
            if [[ $waited -ge 300 ]]; then die "ERR: Timeout waiting for lock. PID: $lock_pid"; fi
            echo "Lock held by PID $lock_pid. Waiting... ($waited/300s)"
            sleep 10; waited=$((waited + 10)); continue
        fi
        
        # Cron logic: fail fast or detect stuck
        local cur_time=$(date +%s)
        local m_time=$(stat -c %Y "$LOCKFILE" 2>/dev/null || echo "$cur_time")
        local age=$((cur_time - m_time))
        if [[ "$age" -gt "$timeout_val" ]]; then
            send_smtp_alert "CRITICAL: Stuck job detected ($((age/60)) min). PID: $lock_pid"
            die "ERR: Stuck job detected."
        else 
            die "ERR: Replication already running. PID: $lock_pid"
        fi
    done
    echo "$$" > "$LOCKFILE"
    trap 'rm -f "$LOCKFILE"' EXIT
}

# Params
raw_dataset=$1
label=${2:-"frequently"}
keep_fallback=${3:-"10"}
[[ -n "$raw_dataset" ]] || die "dataset not specified"
ME=${REPL_ME:-$(hostname)}
ds_name="${raw_dataset#*/}"

# 1. Resolve local pool
configured_pool=$(zfs get -H -o value "repl:node:${ME}:fs" "$raw_dataset" 2>/dev/null | grep -v "^-$")
if [[ -z "$configured_pool" ]]; then
    # If ME is an alias in the chain but doesn't have repl:node:ME:fs, fallback to generic pool or ME-pool
    if [[ -n "$REPL_CHAIN" ]]; then
        IFS=',' read -r -a all_aliases <<< "$REPL_CHAIN"
        for alias_in_chain in "${all_aliases[@]}"; do
            if [[ "$alias_in_chain" == "$ME" ]]; then
                if zfs list pool >/dev/null 2>&1; then configured_pool="pool"; else configured_pool="${ME}-pool"; fi
                break
            fi
        done
    fi
    [[ -z "$configured_pool" ]] && configured_pool="${ME}-pool" # Final fallback if ME not in chain or no generic pool
fi
local_ds="${configured_pool}/${ds_name}"
dataset=$local_ds
# 2. Chain & Role Discovery
REPL_CHAIN=$(get_zfs_prop "repl:chain" "$local_ds")
REPL_USER=$(get_zfs_prop "repl:user" "$local_ds")
[[ -z "$REPL_USER" ]] && REPL_USER="root"
IS_MASTER=false; ME_INDEX=-1; NODES_REMAINING=(); NEXT_HOP=""; nodes=()
if [[ -n "$REPL_CHAIN" ]]; then
    IFS=',' read -r -a nodes <<< "$REPL_CHAIN"
    for i in "${!nodes[@]}"; do
        if [[ "${nodes[i]}" == "$ME" ]]; then
            ME_INDEX=$i; [[ $i -eq 0 ]] && IS_MASTER=true
            for (( j=i+1; j<${#nodes[@]}; j++ )); do NODES_REMAINING+=("${nodes[j]}"); done
            break
        fi
    done
fi

MARK_ONLY=false; initial_send=false; PROMOTE=false; CASCADED=false; SUSPEND=false; RESUME=false; AUTO=false; DESTROY_CHAIN=false; YES=false; PROMOTE_SNAP=""; sync_props_data=""; TARGET_NODE=""; IS_DONOR=false

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

[[ -n "$TARGET_NODE" ]] && { NODES_REMAINING=("$TARGET_NODE"); IS_MASTER=true; }
if [[ "$PROMOTE" == false && "$SUSPEND" == false && "$RESUME" == false && -z "$TARGET_NODE" && "$IS_DONOR" == false ]]; then
    [[ $ME_INDEX -eq -1 ]] && die "ERR: Host $ME not in chain for $local_ds"
fi

# 3. Handle Suspend/Resume
if [[ "$SUSPEND" == true || "$RESUME" == true ]]; then
    ACTION="SUSPENDED"; VAL="true"; [[ "$RESUME" == true ]] && { ACTION="RESUMED"; VAL="false"; }
    echo "${ACTION} replication for $raw_dataset..."
    for n in "${nodes[@]}"; do
        n_fqdn=$(resolve_node_fqdn "$n" "$local_ds"); n_user=$(resolve_node_user "$n" "$local_ds"); n_pool=$(resolve_node_pool "$n" "$local_ds")
        ssh "${n_user}@${n_fqdn}" "zfs set repl:suspend=$VAL ${n_pool}/${ds_name}" || echo "  Warning: Failed on $n"
    done
    send_smtp_alert "NOTICE: Replication ${ACTION}. Master: ${nodes[0]}."
    exit 0
fi

# 4. Handle Promotion
if [[ "$PROMOTE" == true ]]; then
    echo "Promoting $ME to Master..."
    CURRENT_CHAIN=$(get_zfs_prop "repl:chain" "$local_ds")
    [[ -z "$CURRENT_CHAIN" ]] && die "ERR: No repl:chain found."
    IFS=',' read -r -a p_nodes <<< "$CURRENT_CHAIN"
    NEW_P_NODES=("$ME")
    for n in "${p_nodes[@]}"; do [[ "$n" != "$ME" ]] && NEW_P_NODES+=("$n"); done
    NEW_CHAIN=$(IFS=','; echo "${NEW_P_NODES[*]}")
    if [[ "$CURRENT_CHAIN" != "$NEW_CHAIN" ]]; then
        zfs set repl:chain="$NEW_CHAIN" "$local_ds"
        send_smtp_alert "NOTICE: $ME PROMOTED. Chain: $NEW_CHAIN"
        REPL_CHAIN="$NEW_CHAIN"; IS_MASTER=true; ME_INDEX=0
    fi
    if [[ "$AUTO" == true || -n "$PROMOTE_SNAP" || "$DESTROY_CHAIN" == true ]]; then
        TARGET_SNAP=""
        if [[ -n "$PROMOTE_SNAP" ]]; then
            TARGET_SNAP="$PROMOTE_SNAP"; declare -A snap_guids
            for n in "${NEW_P_NODES[@]}"; do
                n_pool=$(resolve_node_pool "$n" "$raw_dataset"); n_fqdn=$(resolve_node_fqdn "$n" "$raw_dataset"); n_user=$(resolve_node_user "$n" "$raw_dataset")
                g=$(ssh "${n_user}@${n_fqdn}" "zfs get -H -o value guid ${n_pool}/${ds_name}@$TARGET_SNAP" 2>/dev/null)
                [[ -z "$g" || "$g" == "-" ]] && die "ERR: Snap @$TARGET_SNAP not on $n"
                snap_guids["$n"]=$g
            done
        else
            tmp_common="/tmp/zfs-common-snaps.$$"
            f_node_flag=true
            for n in "${NEW_P_NODES[@]}"; do
                n_pool=$(resolve_node_pool "$n" "$raw_dataset"); n_fqdn=$(resolve_node_fqdn "$n" "$raw_dataset"); n_user=$(resolve_node_user "$n" "$raw_dataset")
                if [[ "$f_node_flag" == true ]]; then ssh "${n_user}@${n_fqdn}" "zfs list -t snap -H -o name,guid -r ${n_pool}/${ds_name}" 2>/dev/null | awk '{print $1" "$2}' | cut -d'@' -f2 > "$tmp_common"; f_node_flag=false
                else node_tmp="/tmp/zfs-node-snaps.$$"; ssh "${n_user}@${n_fqdn}" "zfs list -t snap -H -o name,guid -r ${n_pool}/${ds_name}" 2>/dev/null | awk '{print $1" "$2}' | cut -d'@' -f2 > "$node_tmp"; grep -Fxf "$tmp_common" "$node_tmp" > "${tmp_common}.new"; mv "${tmp_common}.new" "$tmp_common"; rm -f "$node_tmp"; fi
                [[ ! -s "$tmp_common" ]] && break
            done
            if [[ -s "$tmp_common" ]]; then latest_line=$(tail -n 1 "$tmp_common"); TARGET_SNAP=$(echo "$latest_line" | awk '{print $1}'); fi
            rm -f "$tmp_common"
        fi
        if [[ "$AUTO" == true || -n "$PROMOTE_SNAP" ]]; then
            [[ -z "$TARGET_SNAP" ]] && die "ERR: No common snap."
            [[ "$YES" != true ]] && { echo -n "Rollback to @$TARGET_SNAP? (y/N): "; read -r resp; [[ "$resp" != "y" ]] && die "Aborted."; }
            for n in "${NEW_P_NODES[@]}"; do
                n_pool=$(resolve_node_pool "$n" "$raw_dataset"); n_fqdn=$(resolve_node_fqdn "$n" "$raw_dataset"); n_user=$(resolve_node_user "$n" "$raw_dataset")
                ssh "${n_user}@${n_fqdn}" "zfs rollback -r ${n_pool}/${ds_name}@$TARGET_SNAP" || die "ERR: Rollback failed on $n"
            done
        fi
    fi
fi

if [[ -n "$sync_props_data" && "$PROMOTE" != true ]]; then
    zfs list "$local_ds" >/dev/null 2>&1 && apply_repl_props "$local_ds" "$sync_props_data"
fi

RESOLVED_KEEP=$(resolve_retention "$local_ds" "$label" "$keep_fallback")
echo "Start: $(date); Dataset: $local_ds; Label: $label"

if [[ "$IS_MASTER" == false && "$CASCADED" == false && "$PROMOTE" == false && "$MARK_ONLY" == false && -z "$TARGET_NODE" && "$IS_DONOR" == false ]]; then
    echo "INFO: Not Master. Skipping."; exit 0
fi
if [[ "$IS_MASTER" == true && "$CASCADED" == false && "$PROMOTE" == false && -z "$TARGET_NODE" ]]; then
    [[ "$(get_zfs_prop "repl:suspend" "$local_ds")" == "true" ]] && { echo "INFO: SUSPENDED."; exit 0; }
fi
if [[ "$MARK_ONLY" == true ]]; then
    [[ "$IS_MASTER" == true ]] && purge_shipped_snapshots "$local_ds" "$label" "$RESOLVED_KEEP"
    exit 0
fi

# check_stuck_job

if [[ "$IS_MASTER" == true && "$CASCADED" == false && "$PROMOTE" == false && -z "$TARGET_NODE" && "$IS_DONOR" == false ]]; then
    k_flag=$(cat /var/run/keep-$label.txt 2> /dev/null); [[ -z "$k_flag" ]] && k_flag=999
    /usr/sbin/zfs-auto-snapshot --syslog --label=$label --keep=$k_flag "$local_ds" || die "ERR: snapshot failed"
fi

# Identify local "latest" snapshot for verification (Must happen after creation)
LATEST_SNAP=$(zfs list -t snap -o name -H -S creation -r "$local_ds" | grep "@.*$label" | head -n 1 | cut -d'@' -f2)

REPLICATION_SUCCESS=false
for hop_node in "${NODES_REMAINING[@]}"; do
    hop_fqdn=$(resolve_node_fqdn "$hop_node" "$raw_dataset"); hop_user=$(resolve_node_user "$hop_node" "$raw_dataset"); HOP_TARGET="${hop_user}@${hop_fqdn}"
    NEXT_HOP_POOL=$(resolve_node_pool "$hop_node" "$raw_dataset")
    [[ $? -eq 255 ]] && { echo "ERROR: $hop_node unreachable. Skipping..."; continue; }
    
    zfsbud_opts=""; [[ "$initial_send" == true ]] && zfsbud_opts="-i"
    TRANSFER_DONE=false
    if zfsbud_core $zfsbud_opts -s "$NEXT_HOP_POOL" -e "ssh $HOP_TARGET" -v "$local_ds"; then
        echo "Replication to $HOP_TARGET successful."; TRANSFER_DONE=true
    else
        echo "WARNING: Local failed. Searching donor..."
        DONOR_NODE=$(find_best_donor "$hop_node" "$raw_dataset")
        if [[ -n "$DONOR_NODE" ]]; then
            donor_fqdn=$(resolve_node_fqdn "$DONOR_NODE" "$raw_dataset"); donor_user=$(resolve_node_user "$DONOR_NODE" "$raw_dataset"); donor_target="${donor_user}@${donor_fqdn}"
            if ssh "$donor_target" "zfs-replication.sh $raw_dataset $label $keep_fallback --target $hop_node --donor"; then
                echo "Delegated replication from $DONOR_NODE to $hop_node successful."; TRANSFER_DONE=true
            fi
        fi
    fi

    if [[ "$TRANSFER_DONE" == true ]]; then
        REPLICATION_SUCCESS=true
        casc_opts=""; [[ "$initial_send" == true ]] && casc_opts="--initial"
        PROPS_ARG=$(get_repl_props_encoded "$local_ds")
        DOWNSTREAM_OUT=$(ssh "$HOP_TARGET" "zfs-replication.sh $raw_dataset $label $keep_fallback $casc_opts --sync-props $PROPS_ARG --cascaded" 2>&1)
        SSH_STATUS=$?
        echo "$DOWNSTREAM_OUT" | grep -v "^SENT_LIST:"
        if [[ $SSH_STATUS -eq 0 ]]; then
            ARRIVED_LIST=$(echo "$DOWNSTREAM_OUT" | grep "^SENT_LIST:" | cut -d':' -f2)
            if [[ -n "$LATEST_SNAP" && ",$ARRIVED_LIST," == *",$LATEST_SNAP,"* ]]; then
                echo "VERIFICATION SUCCESS: $LATEST_SNAP reached sink."
            else echo "WARNING: Verification FAILED for $LATEST_SNAP."; fi
        fi
        break
    fi
done

if [[ "$REPLICATION_SUCCESS" == true ]]; then
    zfs list -t snap -o name -H -r "$local_ds" | grep "@.*$label" | while read s; do zfs set zfs-send:shipped=true "$s"; done
    purge_shipped_snapshots "$local_ds" "$label" "$RESOLVED_KEEP"
    MY_LIST=$(zfs list -t snap -o name -H -S creation -r "$local_ds" | grep "@.*$label" | cut -d'@' -f2 | xargs | tr ' ' ',')
    echo "SENT_LIST:$MY_LIST"
elif [[ ${#NODES_REMAINING[@]} -gt 0 ]]; then
    echo 9999 > /var/run/keep-$label.txt; die "ERR: All downstream attempts failed."
else
    if [[ "$IS_DONOR" == true ]]; then echo "INFO: Donor run complete."
    else
        /usr/sbin/zfs-auto-snapshot --syslog --label=$label --keep=$RESOLVED_KEEP "$local_ds"
        zfs list -t snap -o name -H -r "$local_ds" | grep "@.*$label" | while read s; do zfs set zfs-send:shipped=true "$s"; done
        purge_shipped_snapshots "$local_ds" "$label" "$RESOLVED_KEEP"
    fi
    SINK_LIST=$(zfs list -t snap -o name -H -S creation -r "$local_ds" | grep "@.*$label" | cut -d'@' -f2 | xargs | tr ' ' ',')
    echo "SENT_LIST:$SINK_LIST"
fi
echo "Done: $(date)"; exit 0
