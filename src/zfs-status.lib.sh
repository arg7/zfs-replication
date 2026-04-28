#!/bin/bash

# Get node state (internal)
get_node_state() {
    local alias="$1"
    local raw_ds="$2"
    local node_role="$3"  # master, middle, or sink
    local fqdn=$(resolve_node_fqdn "$alias" "$raw_ds")
    local user=$(resolve_node_user "$alias" "$raw_ds")
    local ssh_t=$(resolve_ssh_timeout "$raw_ds")
    local output

    if [[ "$alias" == "$(get_local_alias "$raw_ds" "")" ]]; then
        output=$("$ZEPLICATOR_CMD" --alias "$alias" --stats "$raw_ds" 2>/dev/null)
    else
        output=$(timeout "$ssh_t" ssh -o ConnectTimeout="$ssh_t" -o BatchMode=yes "${user}@${fqdn}" "$ZEPLICATOR_CMD --alias $alias --stats $raw_ds" 2>/dev/null)
    fi

    if [[ -z "$output" ]]; then
        return 1
    fi

    echo "$output"
}

cmd_status() {
    local raw_filesystem="$1"
    local configured_only="${2:-false}"

    if [[ -z "$raw_filesystem" ]]; then
        readarray -t filesystems < <(zfs list -H -o name | while read ds; do if zfs get -H -o value zep:chain "$ds" 2>/dev/null | grep -qv "^-$"; then echo "$ds"; fi; done)
        [[ ${#filesystems[@]} -eq 0 ]] && die "ERR: No filesystems with zep:chain found."
        raw_filesystem="${filesystems[0]}"
    fi
    REPL_CHAIN=$(get_zfs_prop "zep:chain" "$raw_filesystem")
    [[ -z "$REPL_CHAIN" ]] && die "ERR: No replication chain found."
    IFS=',' read -r -a nodes <<< "$REPL_CHAIN"
    
    local global_exit_code=0
    local idx=0

    for n in "${nodes[@]}"; do
        idx=$((idx + 1))
        node_ds=$(resolve_node_filesystem "$n" "$raw_filesystem")
        node_fqdn=$(resolve_node_fqdn "$n" "$raw_filesystem")

        # Determine node role
        local node_role="middle"
        [[ $idx -eq 1 ]] && node_role="master"
        [[ $idx -eq ${#nodes[@]} ]] && node_role="sink"

        # Measure ping to node (local node gets "local")
        local ping_str="local"
        if [[ "$n" != "$(get_local_alias "$raw_filesystem" "")" ]]; then
            local ping_ms
            ping_ms=$(ping -c 1 -W 2 "$node_fqdn" 2>/dev/null | grep -oP 'time[=/]\K[0-9.]+' | head -1 || echo "")
            if [[ -n "$ping_ms" ]]; then
                ping_ms=$(printf "%.0f" "$ping_ms")
                ping_str="${ping_ms}ms"
            else
                ping_str="down"
            fi
        fi

        out=$(get_node_state "$n" "$node_ds" "$node_role")
        
        node_reachable=$?
        
        zpools=$(echo "$out" | grep "^ZPOOL:" | cut -d':' -f2-)
        iostats=$(echo "$out" | grep "^IOSTAT:" | cut -d':' -f2-)
        filesystems_raw=$(echo "$out" | grep "^FILESYSTEM|")
        transfers_raw=$(echo "$out" | grep "^TRANSFER|")

        # Build transfer progress lookup: key=safe_ds, value="pp%"
        declare -A transfer_progress=()
        while IFS='|' read -r _ tds test tactual; do
            [[ -z "$tds" ]] && continue
            if [[ "$test" -gt 0 ]]; then
                local pct=$(( tactual * 100 / test ))
                [[ $pct -gt 100 ]] && pct=100
                transfer_progress["$tds"]="${pct}%"
            else
                transfer_progress["$tds"]="0%"
            fi
        done <<< "$transfers_raw"
        
        # Pre-evaluate filesystem colors to allow hierarchical bubbling
        filesystems=""
        while read -r line; do
            [[ -z "$line" ]] && continue
            IFS='|' read -r _ _ label _ age conf hb has_sb snap_count keep_val <<< "$line"
            [[ "$configured_only" == "true" && "$conf" == "false" ]] && continue
            [[ -z "$snap_count" ]] && snap_count=0
            [[ -z "$keep_val" ]] && keep_val=0

            # Heartbeat Logic
            if [[ -n "$hb" && "$hb" != "-" ]]; then
                if [[ "$hb" =~ ^([0-9]+)m$ ]]; then hb="${BASH_REMATCH[1]}";
                elif [[ "$hb" =~ ^([0-9]+)h$ ]]; then hb=$((${BASH_REMATCH[1]}*60));
                elif [[ "$hb" =~ ^([0-9]+)d$ ]]; then hb=$((${BASH_REMATCH[1]}*1440));
                elif [[ "$hb" =~ ^([0-9]+)M$ ]]; then hb=$((${BASH_REMATCH[1]}*43200));
                elif [[ "$hb" =~ ^([0-9]+)Y$ ]]; then hb=$((${BASH_REMATCH[1]}*525600));
                elif [[ "$hb" =~ ^([0-9]+)$ ]]; then hb="${BASH_REMATCH[1]}";
                else hb=60; fi
            else
                if [[ "$label" =~ ^min([0-9]+)$ ]]; then hb="${BASH_REMATCH[1]}";
                elif [[ "$label" =~ ^hour([0-9]+)$ ]]; then hb=$((${BASH_REMATCH[1]}*60));
                elif [[ "$label" =~ ^day([0-9]+)$ ]]; then hb=$((${BASH_REMATCH[1]}*1440));
                elif [[ "$label" =~ ^month([0-9]+)$ ]]; then hb=$((${BASH_REMATCH[1]}*43200));
                elif [[ "$label" =~ ^year([0-9]+)$ ]]; then hb=$((${BASH_REMATCH[1]}*525600));
                else hb=60; fi
            fi
            
            c_logic="GREEN"; [[ $age -ge $((hb*5)) ]] && c_logic="YELLOW"; [[ $age -ge $((hb*10)) ]] && c_logic="RED"
            
            # Split-Brain forces RED
            [[ "$has_sb" == "true" ]] && c_logic="RED"

            # Retention percentage
            ret_pct=0
            if [[ "$keep_val" -gt 0 && "$snap_count" -gt 0 ]]; then
                ret_pct=$(( snap_count * 100 / keep_val ))
            fi
            ret_color=$C_GREEN
            [[ $ret_pct -lt 60 ]] && ret_color=$C_YELLOW
            [[ $ret_pct -lt 30 ]] && ret_color=$C_RED
            if [[ $ret_pct -ge 100 ]]; then ret_color=$C_GREEN; fi

            filesystems+="${line}|${c_logic}|${ret_pct}|${ret_color}"$'\n'
        done <<< "$filesystems_raw"

        relevant_pools=$(echo "$filesystems" | cut -d'|' -f2 | cut -d'/' -f1 | sort -u)
        
        # Pre-evaluate Zpool statuses
        pool_max_status="GREEN"
        pool_has_check=""
        pool_has_full=""
        pool_has_sb=""
        for p_name in $relevant_pools; do
            p_line=$(echo "$zpools" | awk -v p="$p_name" '$1 == p')
            health=$(echo "$p_line" | awk '{print $2}')
            cap=$(echo "$p_line" | awk '{print $3}' | tr -d '%')
            free_pct=$((100 - cap))

            if [[ "$health" != "ONLINE" ]]; then pool_max_status="RED"; pool_has_check="true"; fi
            if [[ "$pool_max_status" != "RED" && "$cap" -ge 40 ]]; then pool_max_status="YELLOW"; fi
            if [[ "$cap" -ge 80 ]]; then pool_max_status="RED"; fi
            if [[ "$cap" -ge 40 ]]; then pool_has_full="true"; fi
            
            # Bubble split-brain to pool
            if echo "$filesystems" | grep -E "^FILESYSTEM\|$p_name(\||/)" | cut -d'|' -f8 | grep -q "^true$"; then
                pool_max_status="RED"
                pool_has_sb="true"
            fi
        done

        # Aggregate Node status
        n_status="GREEN"
        [[ $node_reachable -ne 0 ]] && n_status="RED"
        [[ "$n_status" != "RED" && "$filesystems" =~ \|RED$'\n' ]] && n_status="RED"
        [[ "$n_status" != "RED" && "$filesystems" =~ \|YELLOW$'\n' ]] && n_status="YELLOW"
        [[ "$n_status" != "RED" && "$pool_max_status" == "RED" ]] && n_status="RED"
        [[ "$n_status" != "RED" && "$pool_max_status" == "YELLOW" ]] && n_status="YELLOW"
        
        # Update global exit code
        if [[ "$n_status" == "RED" ]]; then
            global_exit_code=2
        elif [[ "$n_status" == "YELLOW" && $global_exit_code -lt 1 ]]; then
            global_exit_code=1
        fi
        
        n_desc=""
        if [[ $node_reachable -eq 0 ]]; then
            n_parts=()
            
            fs_parts=()
            [[ "$pool_has_sb" == "true" ]] && fs_parts+=("${C_RED}split-brain${C_RESET}")
            if [[ ${#fs_parts[@]} -gt 0 ]]; then
                n_parts+=("fs: [$(IFS=", "; echo "${fs_parts[*]}")]")
            fi

            snap_parts=()
            if [[ "$filesystems" =~ \|RED$'\n' ]]; then snap_parts+=("${C_RED}stale${C_RESET}")
            elif [[ "$filesystems" =~ \|YELLOW$'\n' ]]; then snap_parts+=("${C_YELLOW}late${C_RESET}")
            fi
            
            if [[ ${#snap_parts[@]} -gt 0 ]]; then
                n_parts+=("snap: [$(IFS=", "; echo "${snap_parts[*]}")]")
            fi
            
            zpool_parts=()
            [[ "$pool_has_check" == "true" ]] && zpool_parts+=("${C_RED}check${C_RESET}")
            if [[ "$pool_has_full" == "true" ]]; then
                if [[ "$pool_max_status" == "RED" ]]; then zpool_parts+=("${C_RED}full${C_RESET}")
                else zpool_parts+=("${C_YELLOW}full${C_RESET}")
                fi
            fi
            
            if [[ ${#zpool_parts[@]} -gt 0 ]]; then
                n_parts+=("zpool: [$(IFS=", "; echo "${zpool_parts[*]}")]")
            fi
            
            if [[ ${#n_parts[@]} -gt 0 ]]; then
                n_desc="  $(IFS=" | "; echo "${n_parts[*]}")"
            fi
        fi

        c_node=$C_GREEN; [[ "$n_status" == "YELLOW" ]] && c_node=$C_YELLOW; [[ "$n_status" == "RED" ]] && c_node=$C_RED

        [[ $idx -gt 1 ]] && echo ""
        ping_color=$C_GREEN; [[ "$ping_str" == "down" ]] && ping_color=$C_RED
        echo -e "${c_node}●${C_RESET} $n (${node_fqdn}, ping: ${ping_color}${ping_str}${C_RESET})${n_desc}"
        [[ $node_reachable -ne 0 ]] && { echo -e "  ${C_RED}  [UNREACHABLE]${C_RESET}"; continue; }
        
        for p_name in $relevant_pools; do
            p_line=$(echo "$zpools" | awk -v p="$p_name" '$1 == p')
            health=$(echo "$p_line" | awk '{print $2}')
            cap=$(echo "$p_line" | awk '{print $3}' | tr -d '%')
            free_pct=$((100 - cap))

            p_status="GREEN"
            p_desc=""
            if [[ "$health" != "ONLINE" ]]; then p_status="RED"; p_desc=" [check]"; fi
            if [[ "$cap" -ge 40 ]]; then 
                if [[ "$p_status" != "RED" ]]; then p_status="YELLOW"; fi
                if [[ "$cap" -ge 80 ]]; then p_status="RED"; fi
                if [[ -z "$p_desc" ]]; then p_desc=" [full]"; else p_desc=", [full]"; fi
            fi
            
            # Aggregate from child filesystems
            ds_statuses=$(echo "$filesystems" | grep "^FILESYSTEM|$p_name")
            [[ "$p_status" != "RED" && "$ds_statuses" =~ \|RED$'\n' ]] && p_status="RED"
            [[ "$p_status" != "RED" && "$ds_statuses" =~ \|YELLOW$'\n' ]] && p_status="YELLOW"

            c_pool=$C_GREEN; [[ "$p_status" == "YELLOW" ]] && c_pool=$C_YELLOW; [[ "$p_status" == "RED" ]] && c_pool=$C_RED
            echo -e "  ${c_pool}💾${C_RESET} $p_name | $health, ${free_pct}% free${p_desc}"

            # IO stats for this pool
            io_line=$(echo "$iostats" | awk -v p="$p_name" '$1 == p')
            if [[ -n "$io_line" ]]; then
                read -r _ _ _ io_r io_w bw_r bw_w <<< "$io_line"
                io_str=""
                [[ -n "$io_r" && "$io_r" != "0" ]] && io_str+="${io_r}r/s "
                [[ -n "$io_w" && "$io_w" != "0" ]] && io_str+="${io_w}w/s "
                [[ -n "$bw_r" && "$bw_r" != "0" ]] && io_str+="${bw_r}R/s "
                [[ -n "$bw_w" && "$bw_w" != "0" ]] && io_str+="${bw_w}W/s"
                io_str="${io_str% }"  # strip trailing space
                if [[ -n "$io_str" ]]; then
                    # Build same prefix as pool line, replace "💾" → " " and " |" → "└─"
                    pool_prefix="  💾 $p_name |"
                    visual_len=$((5 + ${#p_name} + 1))  # "  💾 " + name + space
                    indent=$(printf "%${visual_len}s" "")
                    echo -e "${C_DIM}${indent}└─ io: $io_str${C_RESET}"
                fi
            fi

            # Use process substitution or a read loop from a string to preserve state if needed,
            # but since we already evaluated colors, we just read them.
            while read -r ds_path; do
                [[ -z "$ds_path" ]] && continue
                ds_lines=$(echo "$filesystems" | grep "^FILESYSTEM|$ds_path|")
                
                # Evaluate dataset-level split-brain and status
                has_sb_ds="false"
                if echo "$ds_lines" | cut -d'|' -f8 | grep -q "^true$"; then has_sb_ds="true"; fi
                
                ds_status="GREEN"
                [[ "$ds_lines" =~ \|RED$'\n' ]] && ds_status="RED"
                [[ "$ds_status" != "RED" && "$ds_lines" =~ \|YELLOW$'\n' ]] && ds_status="YELLOW"

                c_ds=$C_GREEN; [[ "$ds_status" == "YELLOW" ]] && c_ds=$C_YELLOW; [[ "$ds_status" == "RED" ]] && c_ds=$C_RED
                
                sb_label=""
                [[ "$has_sb_ds" == "true" ]] && sb_label=" ${C_RED}${C_BLINK}[split-brain]${C_RESET}"
                
                ds_safe="${ds_path//\//-}"
                sync_badge=""
                if [[ -n "${transfer_progress[$ds_safe]+x}" ]]; then
                    sync_badge=" ${C_CYAN}[sync: ${transfer_progress[$ds_safe]}]${C_RESET}"
                fi

                echo -e "    ${c_ds}📁${C_RESET} ${ds_path#$p_name/}${sync_badge}${sb_label}"
                while read -r line; do
                    [[ -z "$line" ]] && continue
                    # FILESYSTEM|ds|label|snap|age|conf|hb|has_sb|snap_count|keep_val|c_logic|ret_pct|ret_color
                    IFS='|' read -r _ ds_f label_f _ age_f conf_f hb_f has_sb_f snap_count keep_val c_logic ret_pct ret_color <<< "$line"

                    [[ "$configured_only" == "true" && "$conf_f" == "false" ]] && continue

                    age_str=$(format_minutes "$age_f")
                    [[ -z "$snap_count" ]] && snap_count=0
                    [[ -z "$keep_val" ]] && keep_val=0
                    label_desc=""
                    if [[ "$c_logic" == "YELLOW" ]]; then label_desc=" [late]"; fi
                    if [[ "$c_logic" == "RED" ]]; then label_desc=" [stale]"; fi

                    ret_str=""
                    if [[ "$ret_pct" -lt 100 && "$ret_pct" -gt 0 ]]; then
                        ret_str=" ${ret_color}[retained ${ret_pct}%]${C_RESET}"
                    elif [[ "$ret_pct" -eq 0 && "$keep_val" -gt 0 ]]; then
                        ret_str=" ${C_RED}[retained 0%]${C_RESET}"
                    fi

                    c_label=$C_GREEN; [[ "$c_logic" == "YELLOW" ]] && c_label=$C_YELLOW; [[ "$c_logic" == "RED" ]] && c_label=$C_RED
                    [[ "$conf_f" == "false" ]] && echo -e "      - ${c_label}●${C_RESET} ${C_DIM}${label_f}(${snap_count})${C_RESET}: [${age_str}]${label_desc} ${C_RED}[unconfigured]${C_RESET}${ret_str}" || echo -e "      - ${c_label}●${C_RESET} ${label_f}(${snap_count}): [${age_str}]${label_desc}${ret_str}"
                done <<< "$ds_lines"
            done < <(echo "$filesystems" | grep -E "^FILESYSTEM\|${p_name}(\||/)" | cut -d'|' -f2 | sort -u)
        done
    done
    
    return $global_exit_code
}
