#!/bin/bash

# ZFS Replication Manager
# Incorporates zfsbud.sh logic for robust ZFS sending/receiving.
# Credit for zfsbud.sh goes to Pawel Ginalski (https://gbyte.dev / gbytedev)

dir=$(dirname "$0")

# --- START OF ZFSBUD CORE (Adapted from zfsbud.sh) ---

zbud_PATH=/usr/bin:/sbin:/bin
zbud_timestamp_format="%Y-%m-%d-%H%M%S"

zbud_msg() { echo "$*" 1>&2; }
zbud_warn() { zbud_msg "WARNING: $*"; }
zbud_die() { zbud_msg "ERROR: $*"; exit 1; }

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
      $remote_shell "zfs list -H -o name" | grep -qx "$1" && return 0
    else
      zfs list -H -o name | grep -qx "$1" && return 0
    fi
    return 1
  }

  set_resume_token() {
    ! dataset_exists "$1" && return 0
    local token="-"
    if [ -n "$remote_shell" ]; then
      token=$($remote_shell "zfs get -H -o value receive_resume_token $1")
    else
      token=$(zfs get -H -o value receive_resume_token "$1")
    fi
    [[ $token ]] && [[ $token != "-" ]] && resume_token=$token
  }

  get_local_snapshots() { zfs list -H -o name -t snapshot | grep "$1@"; }
  get_remote_snapshots() { $remote_shell "zfs list -H -o name -t snapshot | grep $1@"; }
  
  set_source_snapshots() {
    mapfile -t source_snapshots < <(get_local_snapshots "$1")
  }
  
  set_destination_snapshots() {
    if [ -n "$remote_shell" ]; then
      mapfile -t destination_snapshots < <(get_remote_snapshots "$destination_parent_dataset/$1")
    else
      mapfile -t destination_snapshots < <(get_local_snapshots "$destination_parent_dataset/$1")
    fi
  }

  set_common_snapshot() {
    for destination_snapshot in "${destination_snapshots[@]}"; do
      for source_snapshot in "${source_snapshots[@]}"; do
        [[ "${source_snapshot#*@}" == "${destination_snapshot#*@}" ]] && last_snapshot_common=${source_snapshot#*@}
      done
    done
    [ -n "$last_snapshot_common" ] && return 0 || return 1
  }

  send_initial() {
    local latest_snapshot_source=${source_snapshots[-1]}
    zbud_msg "Initial source snapshot (latest): $latest_snapshot_source"
    zbud_msg "Sending initial snapshot to destination..."
    # Map any source pool/dataset to destination_pool/dataset
    local ds_name="${dataset#*/}"
    local remote_ds="${destination_parent_dataset}/${ds_name}"
    
    # Identify LOCAL dataset to send from
    local my_hostname=$(hostname)
    local local_ds="${my_hostname}-pool/${ds_name}"

    if [ -z "$dry_run" ]; then
      if [ -n "$remote_shell" ]; then
        ! zfs send -w -R $verbose "$latest_snapshot_source" | mbuffer -q -r "$RATE" -m "$BUF" | zstd | $remote_shell "zstd -d | zfs recv $resume -F -u $remote_ds" && return 1
      else
        ! zfs send -w -R $verbose "$latest_snapshot_source" | zfs recv $resume -F -u "$remote_ds" && return 1
      fi
    fi
    last_snapshot_common="${latest_snapshot_source#*@}"
  }

  send_incremental() {
    local last_snapshot_source=${source_snapshots[-1]}
    if [[ ${last_snapshot_source#*@} == "$last_snapshot_common" ]]; then
      zbud_msg "Skipping incremental: already up to date."
      return 0
    fi
    zbud_msg "Sending incremental: $last_snapshot_common -> ${last_snapshot_source#*@}"
    local ds_name="${dataset#*/}"
    local remote_ds="${destination_parent_dataset}/${ds_name}"
    
    # Identify LOCAL dataset to send from (e.g. node2-pool/data)
    local my_hostname=$(hostname)
    local local_ds="${my_hostname}-pool/${ds_name}"

    if [ -z "$dry_run" ]; then
      if [ -n "$remote_shell" ]; then
        ! zfs send -w $recursive_send $verbose -i "$local_ds@$last_snapshot_common" "$last_snapshot_source" | mbuffer -q -r "$RATE" -m "$BUF" | zstd | $remote_shell "zstd -d | zfs recv $resume -F -u $remote_ds" && return 1
      else
        ! zfs send -w $recursive_send $verbose -i "$local_ds@$last_snapshot_common" "$last_snapshot_source" | zfs recv $resume -F -u "$remote_ds" && return 1
      fi
    fi
  }

  # Simplified processing for zfs-replication.sh context
  for dataset in "${datasets[@]}"; do
    local ds_name="${dataset#*/}"
    local my_hostname=$(hostname)
    local local_ds="${my_hostname}-pool/${ds_name}"
    
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
    if ! set_common_snapshot; then
       if [ -n "$initial" ]; then
          send_initial || zbud_die "Initial send failed"
       else
          zbud_warn "No common snapshots for $local_ds. Use -i for initial."
          continue
       fi
    fi
    send_incremental || zbud_die "Incremental send failed"
  done
}

# --- END OF ZFSBUD CORE ---

# Helper functions for main script
die() {
    echo "$@"
    exit 1
}

get_zfs_prop() {
    local prop=$1
    local ds=$2
    zfs get -H -o value "$prop" "$ds" 2>/dev/null | grep -v "^-$" | head -n 1
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
EOF
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
    
    if [[ -f "$LOCKFILE" ]]; then
        local lock_pid=$(cat "$LOCKFILE" 2>/dev/null)
        local cur_time=$(date +%s)
        local m_time=$(stat -c %Y "$LOCKFILE" 2>/dev/null || echo "$cur_time")
        local age=$((cur_time - m_time))
        
        if [[ "$age" -gt "$timeout_val" ]]; then
            send_smtp_alert "CRITICAL: ZFS replication job for $dataset ($label) is stuck. Lock file age: $((age/60)) min. Timeout: $((timeout_val/60)) min. PID recorded: $lock_pid"
            die "ERR: Stuck job detected ($age seconds old). Alert sent."
        else
            die "ERR: Replication already running ($age seconds ago). PID: $lock_pid"
        fi
    fi

    echo "$$" > "$LOCKFILE"
    trap 'rm -f "$LOCKFILE"' EXIT
}

# Params
dataset=$1
label=${2:-"frequently"}
keep_fallback=${3:-"10"}
MARK_ONLY=false
initial_send=false
if [[ "$4" == "--mark-only" ]]; then MARK_ONLY=true; fi
if [[ "$4" == "--initial" || "$5" == "--initial" ]]; then initial_send=true; fi

# Identity & Configuration Discovery
REPL_CHAIN=$(get_zfs_prop "repl:chain" "$dataset")
REPL_USER=$(get_zfs_prop "repl:user" "$dataset")
[[ -z "$REPL_USER" ]] && REPL_USER="root"

echo "Start: $(date); Dataset: $dataset; Label: $label"

[[ -n "$dataset" ]] || die "dataset not specified"

ME=$(hostname)
NEXT_HOP=""
IS_MASTER=false
ME_INDEX=-1
RESOLVED_KEEP=$keep_fallback

if [[ -n "$REPL_CHAIN" ]]; then
    IFS=',' read -r -a nodes <<< "$REPL_CHAIN"
    for i in "${!nodes[@]}"; do
        if [[ "${nodes[i]}" == "$ME" ]]; then
            ME_INDEX=$i
            if [[ $i -eq 0 ]]; then IS_MASTER=true; fi
            if (( i < ${#nodes[@]} - 1 )); then
                NEXT_HOP="${REPL_USER}@${nodes[i+1]}"
            fi
            break
        fi
    done
    [[ $ME_INDEX -eq -1 ]] && die "ERR: Host $ME is not part of the replication chain for $dataset"
    
    # Resolve Graduated Retention
    REPL_KEEP_PROP=$(get_zfs_prop "repl:$label" "$dataset")
    if [[ -n "$REPL_KEEP_PROP" ]]; then
        IFS=',' read -r -a k_values <<< "$REPL_KEEP_PROP"
        if [[ -n "${k_values[$ME_INDEX]}" ]]; then
            RESOLVED_KEEP=${k_values[$ME_INDEX]}
            echo "INFO: Using dynamic retention for $label: $RESOLVED_KEEP (Node Index: $ME_INDEX)"
        fi
    fi
fi

if [[ "$MARK_ONLY" == true ]]; then
    if [[ "$IS_MASTER" == true ]]; then
        purge_shipped_snapshots "$dataset" "$label" "$RESOLVED_KEEP"
    fi
    exit 0
fi

# Safety check
check_stuck_job

# 1. Snapshot creation (Master only)
if [[ "$IS_MASTER" == true ]]; then
    k_flag=$(cat /var/run/keep-$label.txt 2> /dev/null)
    [[ -z "$k_flag" ]] && k_flag=999
    
    echo "Creating snapshot for $dataset (label: $label)..."
    /usr/sbin/zfs-auto-snapshot --syslog --label=$label --keep=$k_flag "$dataset"
    [[ $? -eq 0 ]] || die "ERR: snapshot creation failed"
else
    echo "INFO: Not a master host ($ME), skipping snapshot creation."
fi

# Identify local "latest" snapshot for verification
LATEST_SNAP=$(zfs list -t snap -o name -H -S creation -r "$dataset" | grep "@.*$label" | head -n 1 | cut -d'@' -f2)

# 2. Replication & Audit
if [[ -n "$NEXT_HOP" ]]; then
    # Resolve NEXT_HOP pool name (node2 -> node2-pool)
    NEXT_HOP_HOST=${NEXT_HOP#*@}
    NEXT_HOP_POOL="${NEXT_HOP_HOST}-pool"
    
    echo "Replicating $dataset to $NEXT_HOP (Pool: $NEXT_HOP_POOL)..."
    # Call internal zfsbud logic instead of external script
    zfsbud_opts=""
    if [[ "$initial_send" == true ]]; then zfsbud_opts="-i"; fi
    zfsbud_core $zfsbud_opts -s "$NEXT_HOP_POOL" -e "ssh $NEXT_HOP" -v "$dataset"
    
    if [[ $? -ne 0 ]]; then
        echo 9999 > /var/run/keep-$label.txt
        die "ERR: replication to $NEXT_HOP failed"
    else
        rm /var/run/keep-$label.txt 2>/dev/null
        
        # PROPAGATE & VERIFY
        echo "Cascading: triggering downstream chain for $dataset on $NEXT_HOP"
        casc_opts=""
        if [[ "$initial_send" == true ]]; then casc_opts="--initial"; fi
        DOWNSTREAM_OUT=$(ssh "$NEXT_HOP" "zfs-replication.sh $dataset $label $keep_fallback $casc_opts" 2>&1)
        SSH_STATUS=$?
        
        # Bubble up logs
        echo "$DOWNSTREAM_OUT" | grep -v "^SENT_LIST:"
        
        if [[ $SSH_STATUS -eq 0 ]]; then
            ARRIVED_LIST=$(echo "$DOWNSTREAM_OUT" | grep "^SENT_LIST:" | cut -d':' -f2)
            
            if [[ -n "$LATEST_SNAP" && ",$ARRIVED_LIST," == *",$LATEST_SNAP,"* ]]; then
                echo "VERIFICATION SUCCESS: Snapshot $LATEST_SNAP confirmed at the end of the chain."
                
                # HOUSEKEEPING
                echo "Marking local snapshots as shipped..."
                zfs list -t snap -o name -H -r "$dataset" | grep "@.*$label" | \
                while read s; do
                    zfs set zfs-send:shipped=true "$s"
                done
                purge_shipped_snapshots "$dataset" "$label" "$RESOLVED_KEEP"
                
                echo "SENT_LIST:$ARRIVED_LIST"
            else
                send_smtp_alert "CRITICAL: Verification FAILED for $dataset. Snapshot $LATEST_SNAP NOT found in arrival receipt from $NEXT_HOP."
                die "ERR: Audit failed for $LATEST_SNAP"
            fi
        else
            die "ERR: Downstream chain processing failed on $NEXT_HOP (Code: $SSH_STATUS)."
        fi
    fi
else
    echo "INFO: End of chain ($ME). Reporting state."
    /usr/sbin/zfs-auto-snapshot --syslog --label=$label --keep=$RESOLVED_KEEP "$dataset"
    SINK_LIST=$(zfs list -t snap -o name -H -S creation -r "$dataset" | grep "@.*$label" | cut -d'@' -f2 | xargs | tr ' ' ',')
    echo "SENT_LIST:$SINK_LIST"
fi

echo "Done: $(date)"
exit 0
