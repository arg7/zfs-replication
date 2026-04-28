#!/bin/bash

# zfs-transfer.lib.sh - Replication engine functions for Zeplicator

zbud_PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
zbud_timestamp_format="%Y-%m-%d-%H%M%S"

find_best_donor() {
    local target_node=$1
    local ds_raw=$2
    local target_pool=$(resolve_node_pool "$target_node" "$ds_raw")
    
    local ssh_t=$(resolve_ssh_timeout "$ds_raw")
    local proc_t=$(resolve_proc_timeout "$ds_raw")

    log_message "Starting donor discovery for $target_node ($ds_raw)"

    # Iterate ALL nodes in chain to find someone who shares a GUID with target
    for (( k=${#nodes[@]}-1; k>=0; k-- )); do
        local donor_alias="${nodes[k]}"
        [[ "$donor_alias" == "$target_node" ]] && continue
        [[ "$donor_alias" == "$ME" ]] && continue
        
        log_message "Checking potential donor: $donor_alias"
        local donor_fqdn=$(resolve_node_fqdn "$donor_alias" "$ds_raw")
        local donor_user=$(resolve_node_user "$donor_alias" "$ds_raw")
        local donor_target="${donor_user}@${donor_fqdn}"

        local donor_pool=$(resolve_node_pool "$donor_alias" "$ds_raw")
        local ds_on_donor="${donor_pool}/${ds_raw#*/}"
        if [[ "$donor_pool" == *"/"* ]]; then ds_on_donor="$donor_pool"; fi
        
        # Check if donor has snapshots and shares GUID with target
        log_message "  Verifying filesystem $ds_on_donor on $donor_alias..."
        if timeout "$((ssh_t + 5))" ssh -o ConnectTimeout="$ssh_t" "$donor_target" "zfs list -t snapshot -H -r $ds_on_donor >/dev/null 2>&1"; then
            log_message "  Performing capability dry-run on $donor_alias..."
            if timeout "$((proc_t + 5))" ssh -o ConnectTimeout="$ssh_t" "$donor_target" "$ZEPLICATOR_CMD $ds_raw $label 0 --alias $donor_alias --target $target_node --donor --dry-run >/dev/null 2>&1"; then
                log_message "  ✅ $donor_alias selected as best donor."
                echo "$donor_alias"
                return 0
            else
                log_message "  ❌ $donor_alias dry-run failed (no common snapshots with $target_node)."
            fi
        else
            log_message "  ❌ Filesystem or snapshots missing on $donor_alias."
        fi
    done
    log_message "No suitable donor found for $target_node."
    return 1
}

# divergence_report — called via: zep <ds> --divergence-report <common_snap>
# Scans snapshots after the common point for any non-zero written data.
# Prints a divergence report and exits 2 if divergence found, 0 otherwise.
divergence_report() {
    local ds="$1"
    local common_snap="$2"

    if [[ -z "$ds" || -z "$common_snap" ]]; then
        echo "usage: divergence_report <dataset> <common_snapshot>" >&2
        return 1
    fi

    # Ensure chronological order (oldest to newest)
    # Note: intentionally non-recursive — child dataset snapshots would interleave
    # with parent snaps by creation time, causing zfs diff errors and false matches.
    local chron_list
    chron_list=$(zfs list -t snapshot -o name,written -H -S creation "$ds" 2>/dev/null | tac)

    if [[ -z "$chron_list" ]]; then
        echo "ERROR: No snapshots found for dataset '$ds'" >&2
        return 1
    fi

    local found_common="false"
    local prev=""
    local found_divergence="false"
    local report=""

    while read -r name written; do
        [[ -z "$name" ]] && continue
        if [[ "$found_common" == "false" ]]; then
            if [[ "$name" == *"@${common_snap}" ]]; then
                found_common="true"
                prev="$name"
            fi
            continue
        fi
    done <<< "$chron_list"

    if [[ "$found_common" == "false" ]]; then
        echo "ERROR: Common snapshot '@${common_snap}' not found in dataset '$ds'" >&2
        echo "Available snapshots:" >&2
        zfs list -t snapshot -o name -H "$ds" 2>/dev/null | head -n 20 | while read -r n; do
            echo "  $n" >&2
        done
        return 1
    fi

    # We are after common snap — check for writes
    found_divergence="true"
    report+="diff:\n"
    #zfs mount "$ds" 2>/dev/null
    local diff_text
    diff_text=$(zfs diff "$prev" "$ds" 2>/dev/null | head -n 15 | awk '{print "  |  " $0}')
    if [[ -z "$diff_text" ]]; then
	return 0
    fi
    report+="${diff_text}\n"

    echo -e "$report"
    return 2
}

zfsbud_core() {
  local PATH=$zbud_PATH
  local timestamp=$(date "+$zbud_timestamp_format")
  local RATE=20M
  local BUF=64M
  local mbuffer_throttle="-r $RATE"
  local mbuffer_size="$BUF"
  
  # Conditional flag support based on ZFS properties
  local send_flags=""
  local resume=""

  local create remove_old send initial recursive_send recursive_create recursive_destroy remote_shell verbose log dry_run snapshot_label destination_parent_filesystem
  declare -A src_keep_timestamps=() src_kept_timestamps=() dst_keep_timestamps=() dst_kept_timestamps=()
  local source_snapshots=() destination_snapshots=() last_snapshot_common resume_token

  # Parse args for this internal call
  local OPTIND
  while getopts "cs:in e:Rrp:vldL:h" opt; do
    case $opt in
      c) create=1 ;;
      s) send=1; destination_parent_filesystem=$OPTARG ;;
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
  local filesystems=("$@")

  # Helper inner functions
  filesystem_exists() {
    if [ -n "$remote_shell" ]; then
      $remote_shell "zfs list -H -o name" 2>/dev/null | grep -qx "$1" && return 0
    else
      zfs list -H -o name | grep -qx "$1" && return 0
    fi
    return 1
  }

  set_resume_token() {
    ! filesystem_exists "$1" && return 0
    local token="-"
    if [ -n "$remote_shell" ]; then
      token=$($remote_shell "zfs get -H -o value receive_resume_token $1" 2>/dev/null)
    else
      token=$(zfs get -H -o value receive_resume_token "$1")
    fi
    [[ $token ]] && [[ $token != "-" ]] && resume_token=$token
  }

  get_local_snapshots() { 
    zfs list -H -o name,guid -t snapshot | grep "$1@"
    if [[ "$DRY_RUN" == true ]]; then
        # Inject virtual snapshots from upstream
        if [[ -n "$VIRTUAL_SNAPS_INCOMING" ]]; then
            IFS=',' read -ra v_snaps <<< "$VIRTUAL_SNAPS_INCOMING"
            for v in "${v_snaps[@]}"; do
                [[ -z "$v" ]] && continue
                echo -e "${1}@${v}\tVIRTUAL"
            done
        fi
        # Inject locally created virtual snapshot
        if [[ -n "$VIRTUAL_SNAP_CREATED" ]]; then
            echo -e "${1}@${VIRTUAL_SNAP_CREATED}\tVIRTUAL"
        fi
    fi
  }
  
  set_source_snapshots() {
    # format: filesystem@name<tab>guid
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
        
        # If both are virtual or one is virtual, we compare names
        if [[ "$source_guid" == "VIRTUAL" || "$dest_guid" == "VIRTUAL" ]]; then
            if [[ "${source_snap#*@}" == "${dest_snap#*@}" ]]; then
               last_snapshot_common="${source_snap#*@}"
               zbud_msg "  🔍 Found common VIRTUAL snapshot by name: $last_snapshot_common"
               return 0
            fi
            continue
        fi

        if [[ "$source_guid" == "$dest_guid" ]]; then
           last_snapshot_common="${source_snap#*@}"
           zbud_msg "  ${C_CYAN}🔍${C_RESET} Found common snapshot by GUID: $last_snapshot_common (GUID: $source_guid)"
           return 0
        fi
      done
    done
    [ -n "$last_snapshot_common" ] && return 0 || return 1
  }

  send_snapshot() {
    local remote_ds="$1"
    local is_initial="$2"
    local resume_token="$3"
    local local_ds="$filesystem"
    local ssh_t=$(resolve_ssh_timeout "$filesystem")

    # Read debug timeout before any pipeline execution
    local debug_timeout=$(get_zfs_prop "zep:debug:send_timeout" "$local_ds")
    [[ "$debug_timeout" =~ ^[0-9]+[smhdMY]$ ]] && debug_timeout=$(parse_time_to_seconds "$debug_timeout")
    [[ ! "$debug_timeout" =~ ^[0-9]+$ ]] && debug_timeout=""
    local iomon_timeout=""
    [[ "$debug_timeout" =~ ^[0-9]+$ && "$debug_timeout" -gt 0 ]] && iomon_timeout="$debug_timeout"

    # --- RESUME PATH: complete an interrupted transfer ---
    if [ -n "$resume_token" ]; then
        local resume_recv_args=""
        [[ "$REPL_FORCE" == "true" ]] && resume_recv_args="-F $resume_recv_args"

        local prefix=$(get_snap_prefix "$filesystem")
        local alias_val=${CLI_ALIAS:-$(hostname)}
        local lock_path="${LOCKFILE:-/tmp/${prefix}${alias_val}-default.lock}"
        local err_log="${REPL_ERR_FILE:?REPL_ERR_FILE not set}"

        > "${lock_path}.cnt"

        if [[ "$dry_run" == true ]]; then
            zbud_msg "  🔄 [DRY RUN] Would resume send to $remote_ds"
            return 0
        fi

        local recv_cmd
        if [ -n "$remote_shell" ]; then
            recv_cmd="mbuffer -q $mbuffer_throttle -m \"$mbuffer_size\" | zstd | $remote_shell \"zstd -d | zfs recv -u $resume_recv_args $remote_ds\""
        else
            recv_cmd="zfs recv -u $resume_recv_args \"$remote_ds\""
        fi

        local zfs_resume_pipeline="zfs send $verbose -t \"$resume_token\" 2>>\"$err_log\" | iomon \"$lock_path\" 1 $iomon_timeout | $recv_cmd"

        set -o pipefail
        eval "$zfs_resume_pipeline"
        local status=$?
        set +o pipefail

        if [[ $status -ne 0 ]]; then
            zbud_msg "Resume pipeline failed with status $status"
            return $status
        fi
        return 0
    fi

    # Extract snapshot info
    local latest_line=${source_snapshots[-1]}
    local latest_snapshot_source=$(echo "$latest_line" | awk '{print $1}')
    local snap_name="${latest_snapshot_source#*@}"

    # Configure flags based on properties
    local use_raw="false"
    [[ "$(get_zfs_prop "zep:zfs:raw" "$local_ds")" == "true" ]] && use_raw="true"

    local use_resume="false"
    [[ "$(get_zfs_prop "zep:zfs:resume" "$local_ds")" == "true" ]] && use_resume="true"

    # Build send/recv args based on transfer type
    local send_args recv_args label_msg
    if [[ "$is_initial" == "true" ]]; then
      send_args="-R -v"
      recv_args="-o canmount=noauto"
      label_msg="initial"
    else
      send_args="-p -v $recursive_send -i \"$local_ds@$last_snapshot_common\""
      recv_args="-o canmount=noauto"
      label_msg="incremental: $last_snapshot_common -> $snap_name"
    fi

    [[ "$use_raw" == "true" ]] && send_args="-w $send_args"
    [[ "$use_resume" == "true" ]] && recv_args="-s $recv_args"

    # Force flag: initial send always uses -F, incremental only if REPL_FORCE
    if [[ "$is_initial" == "true" ]]; then
      recv_args="-F $recv_args"
    elif [[ "$REPL_FORCE" == "true" ]]; then
      recv_args="-F $recv_args"
    fi

    # Dry run handling
    if [[ "$dry_run" == true ]]; then
      if [[ "$is_initial" == "true" ]]; then
        zbud_msg "  🚀 [DRY RUN] Would send initial snapshot to $remote_ds: $latest_snapshot_source"
        last_snapshot_common="$snap_name"
      else
        zbud_msg "  ${C_CYAN}🚀 [DRY RUN]${C_RESET} Would send incremental: $last_snapshot_common -> $snap_name to $remote_ds"
      fi
      return 0
    fi

    # Pre-send operations
    if [[ "$is_initial" == "true" ]]; then
      zbud_msg "Initial source snapshot (latest): $latest_snapshot_source"
      zbud_msg "Sending initial snapshot to destination $remote_ds..."

      # FORCE CLEANUP of destination ONLY if --destroy-chain is set
      if [[ "$DESTROY_CHAIN" == true ]]; then
        zbud_msg "DESTROY_CHAIN: Cleaning up $remote_ds for initial send..."
        if [ -n "$remote_shell" ]; then
          $remote_shell -o ConnectTimeout="$ssh_t" "zfs destroy -r $remote_ds 2>/dev/null || true"
        else
          zfs destroy -r "$remote_ds" 2>/dev/null || true
        fi
      fi
    else
      zbud_msg "  ${C_CYAN}🚀${C_RESET} Sending incremental: $last_snapshot_common -> $snap_name to $remote_ds"
    fi

    # Setup logging
    local prefix=$(get_snap_prefix "$filesystem")
    local alias_val=${CLI_ALIAS:-$(hostname)}
    local lock_path="${LOCKFILE:-/tmp/${prefix}${alias_val}-default.lock}"
    local err_log="${REPL_ERR_FILE:?REPL_ERR_FILE not set}"

    # Clear stale error log from previous runs or filesystems
    > "${lock_path}.cnt"
    > "$err_log"

    # Update lock file with destination info for progress monitoring (incremental only)
    if [[ "$is_initial" != "true" && -n "$remote_shell" && -n "$LOCKFILE" && -f "$LOCKFILE" ]]; then
      echo "$(cat "$LOCKFILE" | awk '{print $1}') $hop_node $remote_ds" > "$LOCKFILE"
    fi

    # Build recv pipeline tail: remote adds mbuffer + zstd + ssh wrapper; local is bare
    local recv_cmd
    if [ -n "$remote_shell" ]; then
      local remote_zfs_recv="zstd -d | zfs recv -u $recv_args $remote_ds"
      recv_cmd="mbuffer -q $mbuffer_throttle -m \"$mbuffer_size\" 2>>\"$err_log\" | zstd 2>>\"$err_log\" | $remote_shell -o ConnectTimeout=\"$ssh_t\" \"$remote_zfs_recv\" 2>>\"$err_log\""
    else
      recv_cmd="zfs recv -u $recv_args \"$remote_ds\" 2>>\"$err_log\""
    fi

    # Build full pipeline command
    local zfs_send_pipeline="zfs send $send_args \"$latest_snapshot_source\" 2>>\"$err_log\" | iomon \"$lock_path\" 1 $iomon_timeout | $recv_cmd"

    # Execute transfer pipeline
    local status=0
    set -o pipefail
    eval "$zfs_send_pipeline"
    status=$?
    set +o pipefail

    # Error handling for incremental transfers
    if [[ "$is_initial" != "true" && $status -ne 0 ]]; then
      if [[ -s "$err_log" ]] && grep -q "cannot receive incremental stream" "$err_log" && grep -q "has been modified" "$err_log"; then
        zbud_msg "  ${C_RED}🚨${C_RESET} Split-brain detected on destination (pipeline rejected modified filesystem)"
        local hint_msg="${C_BOLD}HINT:${C_RESET}"
        hint_msg+=" Data divergence (Split-Brain) detected on ${remote_ds}.|${C_CYAN}To realign, rollback to the last common snapshot:${C_RESET}|"
        hint_msg+="    zfs rollback -r ${remote_ds}@${last_snapshot_common}"

        local diff_output=""
        local diff_status=0
        if [ -n "$remote_shell" ]; then
            diff_output=$($remote_shell "zep $remote_ds --divergence-report $last_snapshot_common" 2>&1)
            diff_status=$?
        else
            diff_output=$(divergence_report "$remote_ds" "$last_snapshot_common" 2>&1)
            diff_status=$?
        fi
        [[ -n "$diff_output" ]] && echo "$diff_output" | while IFS= read -r line; do zbud_msg "  |  $line"; done

        if [[ "$REPL_FORCE" == "true" ]]; then
            zbud_warn "Divergence detected but force=true — retrying with zfs recv -F"
            local recv_cmd_force
            if [ -n "$remote_shell" ]; then
              local remote_zfs_recv="zstd -d | zfs recv -u -F $recv_args $remote_ds"
              recv_cmd_force="mbuffer -q $mbuffer_throttle -m \"$mbuffer_size\" 2>>\"$err_log\" | zstd 2>>\"$err_log\" | $remote_shell -o ConnectTimeout=\"$ssh_t\" \"$remote_zfs_recv\" 2>>\"$err_log\""
            else
              recv_cmd_force="zfs recv -u -F $recv_args \"$remote_ds\" 2>>\"$err_log\""
            fi
            local zfs_send_retry="zfs send $send_args \"$latest_snapshot_source\" 2>>\"$err_log\" | iomon \"$lock_path\" 1 $iomon_timeout | $recv_cmd_force"
            set -o pipefail
            eval "$zfs_send_retry"
            status=$?
            set +o pipefail
            if [[ $status -ne 0 ]]; then
                zbud_msg "Force retry also failed with status $status"
                if [ -n "$remote_shell" ]; then
                    $remote_shell "zfs set zep:error:split-brain=true $remote_ds" 2>/dev/null
                else
                    zfs set zep:error:split-brain=true "$remote_ds" 2>/dev/null
                fi
                send_smtp_alert "critical" --detail "Split-Brain on $remote_ds — force retry failed. Manual intervention required."
                return $status
            fi
            zbud_msg "  ${C_BLUE}✅${C_RESET} Force retry succeeded — destination overwritten."
        else
            zbud_msg ""
            while IFS= read -r line; do
                zbud_msg "    ${line//|/}"
            done <<< "$(echo -e "${hint_msg//|/\\n}")"
            zbud_msg ""
            echo "$hint_msg" > "${REPL_HINT_FILE:?REPL_HINT_FILE not set}"

            if [ -n "$remote_shell" ]; then
                $remote_shell "zfs set zep:error:split-brain=true $remote_ds" 2>/dev/null && \
                    zbud_msg "  ${C_CYAN}ℹ️${C_RESET}  Marked $remote_ds on destination with split-brain error flag."
            else
                zfs set zep:error:split-brain=true "$remote_ds" 2>/dev/null && \
                    zbud_msg "  ${C_CYAN}ℹ️${C_RESET}  Marked $remote_ds with split-brain error flag."
            fi

            local clean_hint=$(echo -e "${hint_msg//|/\\n}" | sed 's/\x1b\[[0-9;]*m//g')
            local clean_diff=$(echo -e "$diff_output" | sed 's/\x1b\[[0-9;]*m//g')
            local plain_alert="CRITICAL: Split-Brain Data Divergence on $remote_ds"
            plain_alert+=$'\n'
            plain_alert+="$clean_diff"
            plain_alert+=$'\n\n'
            plain_alert+="$clean_hint"
            send_smtp_alert "critical" "$plain_alert"
            return 2
        fi
      elif [[ -s $err_log ]] && ! grep -vq "destination already exists" "$err_log"; then
        zbud_msg "  ⚠️  Destination snapshot already exists. Treating as success."
        status=0
      else
        zbud_msg "Pipeline failed with status $status"
        if [[ -f $err_log ]]; then
          while IFS= read -r line; do zbud_msg "  [STDERR] $line"; done < "$err_log"
        fi
        return $status
      fi
    fi

    # Success logging
    local iomon_size=$(cat "${lock_path}.cnt" 2>/dev/null | numfmt --to=iec 2>/dev/null || echo 0)
    local snap_count=$(grep -c "send from" "$err_log" || echo 0)
    log_message "REPLICATION: Successfully sent $label_msg replication for $local_ds to $remote_ds (snap count: $snap_count, total size: $iomon_size)"

    # Mark the received snapshot as shipped on the remote side
    if [[ -n "$remote_shell" ]]; then
      $remote_shell -o ConnectTimeout="$ssh_t" "zfs set zep:shipped=true $remote_ds@$snap_name" 2>/dev/null || true
    fi

    # Debug delay
    local delay=$(get_zfs_prop "zep:debug:send_delay" "$local_ds")
    if [[ "$delay" =~ ^[0-9]+$ && "$delay" -gt 0 ]]; then
      zbud_msg "  🧪 DEBUG: Sleeping for ${delay}s after zfs send ($label_msg)..."
      sleep "$delay"
    fi

    # Update common snapshot tracking
    if [[ "$is_initial" == "true" ]]; then
      last_snapshot_common="$snap_name"
    fi

    return $status
  }

  # Simplified processing for zeplicator context
  for filesystem in "${filesystems[@]}"; do
    ds_name="${filesystem#*/}"
    local_ds="$filesystem"
    local remote_ds=""
    
    local err_log="${REPL_ERR_FILE:?REPL_ERR_FILE not set}"

    # Clear stale error log from previous runs or filesystems
    > "$err_log"

    # If destination_parent_filesystem is already a full path (contains /), use it as the exact target
    if [[ "$destination_parent_filesystem" == *"/"* ]]; then
        remote_ds="$destination_parent_filesystem"
    else
        remote_ds="${destination_parent_filesystem}/${ds_name}"
    fi
    
    zbud_msg "  ${C_BLUE}📦${C_RESET} Processing $local_ds -> ${destination_parent_filesystem} (Target: ${remote_ds})"
    
    REPL_FORCE=$(get_zfs_prop "zep:zfs:force" "$filesystem")
    [[ "$initial" == "1" ]] && REPL_FORCE="true"

    # Resolve Throttling and Buffering
    local throttle=$(get_zfs_prop "zep:throttle" "$filesystem")
    [[ "$throttle" != "-" ]] && mbuffer_throttle="-R $throttle" || mbuffer_throttle="-r $RATE"
    local m_size=$(get_zfs_prop "zep:mbuffer_size" "$filesystem")
    [[ "$m_size" != "-" ]] && mbuffer_size="$m_size" || mbuffer_size="$BUF"

    set_source_snapshots "$local_ds"
    if ((${#source_snapshots[@]} < 1)); then
       zbud_warn "No snapshots for $local_ds"
       continue
    fi

    set_resume_token "$remote_ds"

    if [ -n "$resume_token" ]; then
       log_message "INFO: Found receive_resume_token ($resume_token) on $remote_ds, resuming interrupted transfer"
       send_snapshot "$remote_ds" "false" "$resume_token" || return $?
    fi

    set_destination_snapshots "$remote_ds"
    local ds_status=$?
    if [[ $ds_status -ne 0 && $ds_status -ne 1 ]]; then
       zbud_msg "Target node unreachable or filesystem listing failed (Status: $ds_status)"
       return $ds_status
    fi

    if ! set_common_snapshot; then
       if [ -n "$initial" ]; then
          send_snapshot "$remote_ds" "true" || return $?
       else
          zbud_warn "No common snapshots for $local_ds."
          send_smtp_alert "warning" "WARNING: No common snapshots for $local_ds to $remote_ds."
          return 1
       fi
    else
        local latest_dest_snap=$(echo "${destination_snapshots[-1]}" | awk '{print $1}')
        if [[ "$latest_dest_snap" != *"$last_snapshot_common" ]]; then
            zbud_msg "  ${C_DIM}ℹ️${C_RESET}  Destination has newer snapshots (e.g. ${latest_dest_snap#*@})."
        fi

        # Already up to date check
        local latest_src_snap=$(echo "${source_snapshots[-1]}" | awk '{print $1}')
        if [[ "$latest_src_snap" == *"$last_snapshot_common" ]]; then
            zbud_msg "  ${C_DIM}⏩${C_RESET} Skipping incremental: already up to date."
            return 0
        fi

        send_snapshot "$remote_ds" "false" || return $?
    fi
  done
  return 0
}
