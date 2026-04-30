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
            if timeout "$((proc_t + 5))" ssh -o ConnectTimeout="$ssh_t" "$donor_target" "zep $ds_raw $label 0 --alias $donor_alias --target $target_node --donor --dry-run >/dev/null 2>&1"; then
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
    local diff_text
    diff_text=$(zfs diff "$prev" "$ds" 2>/dev/null | head -n 15 | awk '{print "  |  " $0}')
    if [[ -n "$diff_text" ]]; then
        report+="${diff_text}\n"
        echo -e "$report"
        return 2
    fi

    # zfs diff failed (likely unmounted) — fallback to referenced size comparison
    local ref_snap ref_live
    ref_snap=$(zfs get -H -p -o value referenced "$prev" 2>/dev/null)
    ref_live=$(zfs get -H -p -o value referenced "$ds" 2>/dev/null)
    if [[ -n "$ref_snap" && -n "$ref_live" && "$ref_snap" != "$ref_live" ]]; then
        local ref_snap_fmt=$(numfmt --to=iec --suffix=B "$ref_snap" 2>/dev/null || echo "${ref_snap}B")
        local delta
        if [[ "$ref_live" -gt "$ref_snap" ]]; then
            delta=$((ref_live - ref_snap))
        else
            delta=0
        fi
        local delta_fmt=$(numfmt --to=iec --suffix=B "$delta" 2>/dev/null || echo "${delta}B")
        report+="  |  ${delta_fmt} of data written since common snapshot @${common_snap}\n"
        echo -e "$report"
        return 2
    fi

    return 0
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
    $remote_shell zfs list -H -o name 2>/dev/null | grep -qx "$1" && return 0
    return 1
  }

  set_resume_token() {
    ! filesystem_exists "$1" && return 0
    local token="-"
    token=$($remote_shell zfs get -H -o value receive_resume_token "$1" 2>/dev/null)
    [[ $token ]] && [[ $token != "-" ]] && resume_token=$token
  }

  set_source_snapshots() {
    # format: filesystem@name<tab>guid
    mapfile -t source_snapshots < <(zfs list -H -o name,guid -t snapshot | grep "$1@")
    if [[ "$DRY_RUN" == true ]]; then
        if [[ -n "$VIRTUAL_SNAPS_INCOMING" ]]; then
            IFS=',' read -ra v_snaps <<< "$VIRTUAL_SNAPS_INCOMING"
            for v in "${v_snaps[@]}"; do
                [[ -z "$v" ]] && continue
                source_snapshots+=("${1}@${v}\tVIRTUAL")
            done
        fi
        if [[ -n "$VIRTUAL_SNAP_CREATED" ]]; then
            source_snapshots+=("${1}@${VIRTUAL_SNAP_CREATED}\tVIRTUAL")
        fi
    fi
  }
  
  set_destination_snapshots() {
    local target_ds="$1"
    local output
    output=$($remote_shell zfs list -H -o name,guid -t snapshot -r "$target_ds" 2>/dev/null)
    local status=$?
    [[ $status -ne 0 && $status -ne 1 ]] && return $status
    if [[ -n "$output" ]]; then
        mapfile -t destination_snapshots <<< "$output"
    else
        destination_snapshots=()
    fi
    if [[ "$DRY_RUN" == true ]]; then
        if [[ -n "$VIRTUAL_SNAPS_INCOMING" ]]; then
            IFS=',' read -ra v_snaps <<< "$VIRTUAL_SNAPS_INCOMING"
            for v in "${v_snaps[@]}"; do
                [[ -z "$v" ]] && continue
                destination_snapshots+=("${target_ds}@${v}\tVIRTUAL")
            done
        fi
        if [[ -n "$VIRTUAL_SNAP_CREATED" ]]; then
            destination_snapshots+=("${target_ds}@${VIRTUAL_SNAP_CREATED}\tVIRTUAL")
        fi
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

  verify_target() {
    local target_ds="$1"
    local pool="${target_ds%%/*}"

    # 1. Pool check
    if ! $remote_shell zpool list -H -o name "$pool" >/dev/null 2>&1; then
        zbud_msg "  ${C_RED}❌ ERROR:${C_RESET} Pool '$pool' not found on target node."
        return $EXIT_NO_POOL
    fi

    # 2. Dataset existence
    local ds_exists
    if $remote_shell zfs list -H -o name "$target_ds" >/dev/null 2>&1; then
        ds_exists=true
    else
        ds_exists=false
    fi

    # ---- Permission helpers ----
    _check_pool_perms() {
        local user="$1" p="$2"
        local perms
        perms=$($remote_shell zfs allow "$p" 2>/dev/null | grep "user $user " | awk '{print $NF}')
        local missing=()
        for perm in create mount receive userprop; do
            [[ ",$perms," != *",$perm,"* ]] && missing+=("$perm")
        done
        if [[ ${#missing[@]} -gt 0 ]]; then
            zbud_msg "  ${C_RED}❌ Missing pool permissions on $p:${C_RESET} ${missing[*]}"
            return 1
        fi
        return 0
    }

    # Extract SSH user from remote_shell for permission checks
    local check_user="root"
    if [[ -n "$remote_shell" ]]; then
        check_user=$(echo "$remote_shell" | grep -oP '\S+@\S+' | cut -d@ -f1)
        [[ -z "$check_user" ]] && check_user="root"
    fi

    if [[ "$ds_exists" != true ]]; then
        # Dataset doesn't exist — initial send expected
        if [[ -n "$remote_shell" ]]; then
            _check_pool_perms "$check_user" "$pool" || return $EXIT_NO_PERMS
        fi
        zbud_msg "  ${C_DIM}ℹ️${C_RESET}  Target dataset does not exist, will be created by receive."
        return $EXIT_NO_DATASET
    fi

    # 3. Dataset exists — verify pool permissions for userprop (needed after recv)
    if [[ -n "$remote_shell" ]]; then
        _check_pool_perms "$check_user" "$pool" || return $EXIT_NO_PERMS
    fi

    # 4. No snapshots on destination — clean dataset, safe for init
    if [[ ${#destination_snapshots[@]} -eq 0 ]]; then
        if [[ -n "$initial" ]]; then
            zbud_msg "  ${C_DIM}ℹ️${C_RESET}  Target dataset exists but has no snapshots — clean init."
            return $EXIT_NO_DATASET
        fi
        return 0
    fi

    # 5. Has snapshots — check for common ground
    if ! set_common_snapshot; then
        # FOREIGN dataset: has snapshots, no common ground
        zbud_msg "  ${C_RED}❌ FOREIGN DATASET:${C_RESET} $target_ds has snapshots but no common ground with source."
        zbud_msg "  ${C_YELLOW}Remote snapshots ($(printf '%s\n' "${destination_snapshots[@]}" | awk '{print $1}' | grep -c .) total):${C_RESET}"
        printf '%s\n' "${destination_snapshots[@]}" | awk '{print "    "$1}' | head -n 20
        local remaining=$(printf '%s\n' "${destination_snapshots[@]}" | awk '{print $1}' | grep -c .)
        if [[ $remaining -gt 20 ]]; then
            zbud_msg "    ... and $((remaining - 20)) more"
        fi
        zbud_msg "  ${C_YELLOW}Manual intervention required — verify this is the correct target or destroy it.${C_RESET}"
        return 1
    fi

    # 6. Common ground exists — check for divergence on remote
    local common_snap_name="${last_snapshot_common#*@}"
    if [[ -n "$remote_shell" ]]; then
        local div_output div_rc
        div_output=$($remote_shell "zep --divergence-report $common_snap_name $target_ds" 2>&1)
        div_rc=$?

        if [[ $div_rc -eq 2 ]]; then
            echo -e "${CHAIN_PREFIX}  ${C_YELLOW}⚠️  DIVERGENCE DETECTED${C_RESET} on $target_ds since snapshot @$common_snap_name"
            echo -e "$div_output" | while IFS= read -r line; do
                [[ -z "$line" ]] && continue
                echo -e "${CHAIN_PREFIX}  | $line"
            done

            if [[ "$YES" != true ]]; then
                if [[ -t 0 ]]; then
                    echo -ne "${CHAIN_PREFIX}  ${C_YELLOW}Sync will discard this data on target. Continue? [y/N] ${C_RESET}"
                    read -r resp
                    if [[ "$resp" != "y" && "$resp" != "Y" ]]; then
                        zbud_msg "  ${C_YELLOW}⚠️${C_RESET} Aborted by user."
                        return 1
                    fi
                else
                    zbud_msg "  ${C_RED}❌ ERROR:${C_RESET} Divergence detected but no TTY. Use -y to force."
                    return 1
                fi
            fi
            zbud_msg "  ${C_DIM}ℹ️${C_RESET}  Forcing alignment — remote data since @$common_snap_name will be discarded."
        elif [[ $div_rc -ne 0 ]]; then
            zbud_msg "  ${C_YELLOW}⚠️${C_RESET} Divergence check failed (code $div_rc), proceeding anyway."
        fi
    fi

    return 0
  }

  send_snapshot() {
    local remote_ds="$1"
    local is_initial="$2"
    local resume_token="$3"
    local local_ds="$filesystem"
    local ssh_t=$(resolve_ssh_timeout "$filesystem")

    local debug_timeout=$(get_zfs_prop "zep:debug:send_timeout" "$local_ds")
    [[ "$debug_timeout" =~ ^[0-9]+[smhdMY]$ ]] && debug_timeout=$(parse_time_to_seconds "$debug_timeout")
    [[ ! "$debug_timeout" =~ ^[0-9]+$ ]] && debug_timeout=""
    local iomon_timeout=""
    [[ "$debug_timeout" =~ ^[0-9]+$ && "$debug_timeout" -gt 0 ]] && iomon_timeout="$debug_timeout"

    # --- Compute send/recv options ---
    local send_opt recv_opt transfer_label snap_name=""
    local send_extra=$(get_zfs_prop "zep:zfs:send_opt" "$local_ds")
    local recv_extra=$(get_zfs_prop "zep:zfs:recv_opt" "$local_ds")

    if [[ -n "$resume_token" ]]; then
        send_opt="-v -t \"$resume_token\""
        recv_opt="-u${recv_extra:+ $recv_extra}"
        transfer_label="resuming"
    else
        local latest_line=${source_snapshots[-1]}
        local latest_snapshot_source=$(echo "$latest_line" | awk '{print $1}')
        snap_name="${latest_snapshot_source#*@}"
        recv_opt="-u -o canmount=noauto${recv_extra:+ $recv_extra}"

        if [[ "$is_initial" == "true" ]]; then
            send_opt="-v $recursive_send${send_extra:+ $send_extra} \"$latest_snapshot_source\""
            transfer_label="initial: $snap_name"
        else
            send_opt="-v $recursive_send -i \"$local_ds@$last_snapshot_common\"${send_extra:+ $send_extra} \"$latest_snapshot_source\""
            transfer_label="incremental: $last_snapshot_common -> $snap_name"
        fi
    fi

    # --- Unified dry run ---
    if [[ "$dry_run" == true ]]; then
        zbud_msg "  🚀 [DRY RUN] Would send $transfer_label replication to $remote_ds"
        [[ "$is_initial" == "true" ]] && last_snapshot_common="$snap_name"
        return 0
    fi

    # --- Setup ---
    local prefix=$(get_snap_prefix "$filesystem")
    local alias_val=${CLI_ALIAS:-$(hostname)}
    local lock_path="${LOCKFILE:-/tmp/${prefix}${alias_val}-default.lock}"
    local err_log="${REPL_ERR_FILE:?REPL_ERR_FILE not set}"

    > "${lock_path}.cnt"
    > "$err_log"

    if [[ -n "$LOCKFILE" && -f "$LOCKFILE" ]]; then
        local node_info="${hop_node:-$alias_val}"
        echo "$(awk '{print $1}' "$LOCKFILE") $node_info $remote_ds" > "$LOCKFILE"
    fi

    # --- Unified message ---
    zbud_msg "  🚀 Sending $transfer_label replication to $remote_ds..."

    # --- Build pipeline ---
    local iomon_rate=$(get_zfs_prop "zep:debug:throttle" "$local_ds")
    [[ "$iomon_rate" == "-" ]] && iomon_rate=""
    log_message "IOMON: lock=$lock_path interval=1 timeout=$iomon_timeout rate=$iomon_rate"
    local pipeline="zfs send $send_opt 2>>\"$err_log\" | iomon \"$lock_path\" 1 $iomon_timeout $iomon_rate | mbuffer -q $mbuffer_throttle -m \"$mbuffer_size\" 2>>\"$err_log\""
    if [[ -n "$remote_shell" ]]; then
        pipeline+=" | zstd 2>>\"$err_log\" | $remote_shell -o ConnectTimeout=\"$ssh_t\" \"zstd -d | zfs recv $recv_opt $remote_ds\" 2>>\"$err_log\""
    else
        pipeline+=" | zfs recv $recv_opt \"$remote_ds\" 2>>\"$err_log\""
    fi

    # --- Execute ---
    local status=0
    set -o pipefail
    log_message "PIPELINE: $pipeline"
    eval "$pipeline"
    status=$?
    set +o pipefail

    # --- Unified error handling ---
    if [[ $status -ne 0 ]]; then
        if [[ -s "$err_log" ]] && grep -q "cannot receive incremental stream" "$err_log" && grep -q "has been modified" "$err_log"; then
            zbud_msg "  ${C_RED}🚨${C_RESET} Split-brain detected on destination (pipeline rejected modified filesystem)"
            local hint_msg="${C_BOLD}HINT:${C_RESET}"
            hint_msg+=" Data divergence (Split-Brain) detected on ${remote_ds}.|${C_CYAN}To realign, rollback to the last common snapshot:${C_RESET}|"
            hint_msg+="    zfs rollback -r ${remote_ds}@${last_snapshot_common:-?}"

            local diff_output=""
            diff_output=$($remote_shell zep "$remote_ds" --divergence-report "${last_snapshot_common:-}" 2>&1)
            [[ -n "$diff_output" ]] && echo "$diff_output" | while IFS= read -r line; do zbud_msg "  |  $line"; done

            zbud_msg ""
            while IFS= read -r line; do
                zbud_msg "    ${line//|/}"
            done <<< "$(echo -e "${hint_msg//|/\\n}")"
            zbud_msg ""
            echo "$hint_msg" > "${REPL_HINT_FILE:?REPL_HINT_FILE not set}"

            $remote_shell zfs set zep:error:split-brain=true "$remote_ds" 2>/dev/null && \
                zbud_msg "  ${C_CYAN}ℹ️${C_RESET}  Marked $remote_ds with split-brain error flag."
            return 2
        elif [[ -s "$err_log" ]] && grep -q "cannot resume" "$err_log"; then
            zbud_msg "  ${C_RED}🔄${C_RESET} Resume token invalidated — source snapshots destroyed mid-transfer."
            $remote_shell zfs recv -A "$remote_ds" 2>/dev/null && \
                zbud_msg "  ${C_CYAN}ℹ️${C_RESET}  Destroyed stale resume token on $remote_ds."
            return 6
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

    # --- Unified success logging ---
    local iomon_size=$(cat "${lock_path}.cnt" 2>/dev/null | numfmt --to=iec 2>/dev/null || echo 0)
    log_message "REPLICATION: Successfully sent $transfer_label replication for $local_ds to $remote_ds (total size: $iomon_size)"

    local delay=$(get_zfs_prop "zep:debug:send_delay" "$local_ds")
    if [[ "$delay" =~ ^[0-9]+$ && "$delay" -gt 0 ]]; then
        zbud_msg "  🧪 DEBUG: Sleeping for ${delay}s after zfs send ($transfer_label)..."
        sleep "$delay"
    fi

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

    verify_target "$remote_ds"
    local vt_rc=$?

    case $vt_rc in
        $EXIT_NO_DATASET)
            if [ -n "$initial" ]; then
                send_snapshot "$remote_ds" "true" || return $?
            else
                zbud_warn "No dataset on target for $remote_ds. Use --init for initial replication."
                return $vt_rc
            fi
            ;;
        $EXIT_NO_POOL|$EXIT_NO_PERMS)
            return $vt_rc
            ;;
        0)
            if [ -n "$last_snapshot_common" ]; then
                local latest_src_snap=$(echo "${source_snapshots[-1]}" | awk '{print $1}')
                if [[ "$latest_src_snap" == *"$last_snapshot_common" ]]; then
                    zbud_msg "  ${C_DIM}⏩${C_RESET} Skipping incremental: already up to date."
                    return 0
                fi
                local latest_dest_snap=$(echo "${destination_snapshots[-1]}" | awk '{print $1}')
                if [[ "$latest_dest_snap" != *"$last_snapshot_common" ]]; then
                    zbud_msg "  ${C_DIM}ℹ️${C_RESET}  Destination has newer snapshots (e.g. ${latest_dest_snap#*@})."
                fi
                send_snapshot "$remote_ds" "false" || return $?
            elif [ -n "$initial" ]; then
                send_snapshot "$remote_ds" "true" || return $?
            else
                zbud_warn "No common snapshots for $local_ds."
                return 1
            fi
            ;;
        *)
            return $vt_rc
            ;;
    esac
  done
  return 0
}
