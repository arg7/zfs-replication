#!/bin/bash

# zfs-transfer.lib.sh - Replication engine functions for Zeplicator

zbud_PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
zbud_timestamp_format="%Y-%m-%d-%H%M%S"

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
            if ssh "$donor_target" "/scripts/zeplicator $ds_raw $label 0 --alias $donor_alias --target $target_node --donor >/dev/null 2>&1"; then
                echo "$donor_alias"
                return 0
            fi
        fi
    done
    return 1
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
  
  set_source_snapshots() {
    # format: dataset@name<tab>guid
    mapfile -t source_snapshots < <(get_local_snapshots "$1")
  }
  
  set_destination_snapshots() {
    local target_ds="$1"
    if [ -n "$remote_shell" ]; then
      local output
      output=$($remote_shell "zfs list -H -o name,guid -t snapshot -r $target_ds 2>/dev/null" | awk '{print $1" "$2}')
      local status=$?
      [[ $status -ne 0 && $status -ne 1 ]] && return $status # Connectivity error
      mapfile -t destination_snapshots <<< "$output"
    else
      mapfile -t destination_snapshots < <(get_local_snapshots "$target_ds")
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
      
      [[ -z "$dest_guid" || "$dest_guid" == "-" ]] && continue
      
      for source_line in "${source_snapshots[@]}"; do
        source_snap=$(echo "$source_line" | awk '{print $1}')
        source_guid=$(echo "$source_line" | awk '{print $2}')
        
        [[ -z "$source_guid" || "$source_guid" == "-" ]] && continue
        
        if [[ "$source_guid" == "$dest_guid" ]]; then
           last_snapshot_common="${source_snap#*@}"
           zbud_msg "  🔍 Found common snapshot by GUID: $last_snapshot_common (GUID: $source_guid)"
           return 0
        fi
      done
    done
    [ -n "$last_snapshot_common" ] && return 0 || return 1
  }

  send_initial() {
    local latest_line=${source_snapshots[-1]}
    local latest_snapshot_source=$(echo "$latest_line" | awk '{print $1}')
    local remote_ds="$1"
    zbud_msg "Initial source snapshot (latest): $latest_snapshot_source"
    zbud_msg "Sending initial snapshot to destination $remote_ds..."
    
    # Identify LOCAL dataset to send from
    local local_ds="$dataset"

    local timeout_val=$(get_zfs_prop "repl:timeout" "$dataset")
    [[ -z "$timeout_val" || "$timeout_val" == "-" ]] && timeout_val="3600"

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
    local remote_ds="$1"
    if [[ ${latest_snapshot_source#*@} == "$last_snapshot_common" ]]; then
      zbud_msg "  ⏩ Skipping incremental: already up to date."
      return 0
    fi
    zbud_msg "  🚀 Sending incremental: $last_snapshot_common -> ${latest_snapshot_source#*@} to $remote_ds"
    
    # Identify LOCAL dataset to send from
    local local_ds="$dataset"

    local timeout_val=$(get_zfs_prop "repl:timeout" "$dataset")
    [[ -z "$timeout_val" || "$timeout_val" == "-" ]] && timeout_val="3600"

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

  # Simplified processing for zeplicator context
  for dataset in "${datasets[@]}"; do
    ds_name="${dataset#*/}"
    local_ds="$dataset"
    local remote_ds=""

    # If destination_parent_dataset is already a full path (contains /), use it as the exact target
    if [[ "$destination_parent_dataset" == *"/"* ]]; then
        remote_ds="$destination_parent_dataset"
    else
        remote_ds="${destination_parent_dataset}/${ds_name}"
    fi
    
    zbud_msg "  📦 Processing $local_ds -> ${destination_parent_dataset} (Target: ${remote_ds})"
    
    set_source_snapshots "$local_ds"
    if ((${#source_snapshots[@]} < 1)); then
       zbud_warn "No snapshots for $local_ds"
       continue
    fi

    set_resume_token "$remote_ds"
    
    if [ -n "$resume_token" ]; then
       # Resume logic simplified
       if [ -z "$dry_run" ]; then
         if [ -n "$remote_shell" ]; then
           zfs send -w $verbose -t "$resume_token" | mbuffer -q -r "$RATE" -m "$BUF" | zstd | $remote_shell "zstd -d | zfs recv $resume -F -u ${remote_ds}"
         else
           zfs send -w $verbose -t "$resume_token" | zfs recv $resume -F -u "${remote_ds}"
         fi
       fi
    fi

    set_destination_snapshots "$remote_ds"
    local ds_status=$?
    if [[ $ds_status -ne 0 && $ds_status -ne 1 ]]; then
       zbud_msg "Target node unreachable or dataset listing failed (Status: $ds_status)"
       return $ds_status
    fi

    if ! set_common_snapshot; then
       if [ -n "$initial" ]; then
          send_initial "$remote_ds" || return 1
       else
          zbud_warn "No common snapshots for $local_ds. Use -i for initial."
          continue
       fi
    else
       # CHECK DATA DIVERGENCE (Split-Brain Safety Check)
       local diff_output=""
       if [ -n "$remote_shell" ]; then
           diff_output=$($remote_shell "zfs diff $remote_ds@$last_snapshot_common $remote_ds | head -n 20" 2>/dev/null)
       else
           diff_output=$(zfs diff "$remote_ds@$last_snapshot_common" "$remote_ds" 2>/dev/null | head -n 20)
       fi
       
       if [[ -n "$diff_output" ]]; then
           zbud_msg "🚨 FATAL: Data divergence (Split-Brain) detected on $remote_ds!"
           zbud_msg "🚨 Production data was written after $last_snapshot_common."
           zbud_msg "🚨 Changed files (preview):"
           while IFS= read -r line; do zbud_msg "  $line"; done <<< "$diff_output"
           zbud_msg "🚨 Aborting replication to prevent silent data destruction!"
           
           # Get offending snapshots if any, to include in the alert
           local offending_snaps=""
           local latest_dest_snap=$(echo "${destination_snapshots[-1]}" | awk '{print $1}')
           if [[ "$latest_dest_snap" != *"$last_snapshot_common" ]]; then
               local found_common=false
               for dest_line in "${destination_snapshots[@]}"; do
                   local dest_s=$(echo "$dest_line" | awk '{print $1}')
                   if [[ "$found_common" == true ]]; then
                       offending_snaps+="\n  - $dest_s"
                   elif [[ "$dest_s" == *"$last_snapshot_common" ]]; then
                       found_common=true
                   fi
               done
           fi
           
           local alert_msg="CRITICAL: Split-Brain Data Divergence on $remote_ds\nData was written to $remote_ds after $last_snapshot_common. Replication aborted to prevent data loss.\n\nData Divergence (preview):\n$diff_output"
           if [[ -n "$offending_snaps" ]]; then
               alert_msg+="\n\nSnapshot Divergence (offending snapshots):$offending_snaps"
           fi
           # Write to error log to ensure it's picked up by send_smtp_alert 'Error Details' block
           echo -e "$alert_msg" > /tmp/zfs-replication.err
           send_smtp_alert "CRITICAL: Split-Brain Data Divergence on $remote_ds"
           return 1
       fi

       # RESOLVE SNAPSHOT DIVERGENCE: Rollback receiver to the common snapshot ONLY if there are newer snapshots
       local latest_dest_snap=$(echo "${destination_snapshots[-1]}" | awk '{print $1}')
       if [[ "$latest_dest_snap" != *"$last_snapshot_common" ]]; then
           zbud_msg "Divergence detected. Rolling back $remote_ds to $last_snapshot_common. Offending snapshots:"
           
           # Print offending snapshots that are about to be destroyed
           local found_common=false
           for dest_line in "${destination_snapshots[@]}"; do
               local dest_s=$(echo "$dest_line" | awk '{print $1}')
               if [[ "$found_common" == true ]]; then
                   zbud_msg "  ❌ $dest_s"
               elif [[ "$dest_s" == *"$last_snapshot_common" ]]; then
                   found_common=true
               fi
           done
           
           if [ -n "$remote_shell" ]; then
             $remote_shell "zfs rollback -r $remote_ds@$last_snapshot_common" || return 1
           else
             zfs rollback -r "$remote_ds@$last_snapshot_common" || return 1
           fi
       fi
       send_incremental "$remote_ds" || return 1
    fi
  done
  return 0
}
