#!/bin/bash

# Get node state (internal)
get_node_state() {
    local alias="$1"
    local raw_ds="$2"
    local fqdn=$(resolve_node_fqdn "$alias" "$raw_ds")
    local user=$(resolve_node_user "$alias" "$raw_ds")
    local ssh_t=$(resolve_ssh_timeout "$raw_ds")

    # The command to run on the node (local or remote)
    local cmd='
        # 1. Zpools
        zpool list -H -o name,health,capacity 2>/dev/null | while read -r line; do
            echo "ZPOOL:$line"
        done

        # 2. Filesystems with zep: properties
        zfs list -H -o name -r "'$raw_ds'" 2>/dev/null | while read -r ds; do
            props=$(zfs get all -H -o property,value "$ds" 2>/dev/null | grep "^zep:")
            if [[ -n "$props" ]]; then
                # Find unique labels for this filesystem
                snap_list=$(zfs list -t snap -o name,creation -p -H -S creation -r "$ds" 2>/dev/null | grep "zeplicator_")
                labels=$(echo "$snap_list" | awk "{print \$1}" | cut -d"@" -f2 | cut -d"_" -f2 | cut -d"-" -f1 | sort -u)
                for label in $labels; do
                    [[ -z "$label" ]] && continue
                    
                    # Get configured status and heartbeat
                    is_configured="false"
                    heartbeat=$(echo "$props" | grep ":alert:heartbeat:${label}" | cut -f2)
                    if echo "$props" | grep -q ":keep:${label}"; then
                        is_configured="true"
                    fi

                    latest=$(echo "$snap_list" | grep "zeplicator_${label}-" | head -n 1)
                    if [[ -n "$latest" ]]; then
                        snap_name=$(echo "$latest" | awk "{print \$1}")
                        then=$(echo "$latest" | awk "{print \$2}")
                        # Calculate age in minutes
                        now=$(date +%s)
                        if [[ -n "$then" ]]; then
                            age=$(( (now - then) / 60 ))
                            
                            # Check for split-brain error flag
                            has_sb=$(echo "$props" | grep ":error:split-brain" | cut -f2)
                            [[ "$has_sb" != "true" ]] && has_sb="false"
                            
                            echo "FILESYSTEM|$ds|$label|$snap_name|$age|$is_configured|$heartbeat|$has_sb"
                        fi
                    fi
                done
            fi
        done
    '
    
    local output
    if [[ "$alias" == "$(get_local_alias "$raw_ds" "")" ]]; then
        output=$(eval "$cmd" 2>/dev/null)
    else
        output=$(timeout "$ssh_t" ssh -o ConnectTimeout="$ssh_t" -o BatchMode=yes "${user}@${fqdn}" "$cmd" 2>/dev/null)
    fi

    if [[ -z "$output" ]]; then
        return 1
    fi

    echo "$output"
}

cmd_status() {
    local raw_filesystem="$1"
    
    if [[ -z "$raw_filesystem" ]]; then
        readarray -t filesystems < <(zfs list -H -o name | while read ds; do if zfs get -H -o value zep:chain "$ds" 2>/dev/null | grep -qv "^-$"; then echo "$ds"; fi; done)
        [[ ${#filesystems[@]} -eq 0 ]] && die "ERR: No filesystems with zep:chain found."
        raw_filesystem="${filesystems[0]}"
    fi
    REPL_CHAIN=$(get_zfs_prop "zep:chain" "$raw_filesystem")
    [[ -z "$REPL_CHAIN" ]] && die "ERR: No replication chain found."
    IFS=',' read -r -a nodes <<< "$REPL_CHAIN"
    
    local global_exit_code=0

    for n in "${nodes[@]}"; do
        node_ds=$(resolve_node_filesystem "$n" "$raw_filesystem")
        out=$(get_node_state "$n" "$node_ds")
        
        node_reachable=$?
        
        zpools=$(echo "$out" | grep "^ZPOOL:" | cut -d':' -f2-)
        filesystems_raw=$(echo "$out" | grep "^FILESYSTEM|")
        
        # Pre-evaluate filesystem colors to allow hierarchical bubbling
        filesystems=""
        while read -r line; do
            [[ -z "$line" ]] && continue
            IFS='|' read -r _ _ label _ age conf hb has_sb <<< "$line"
            
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
            
            filesystems+="${line}|${c_logic}"$'\n'
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
            
            if [[ "$health" != "ONLINE" ]]; then pool_max_status="RED"; pool_has_check="true"; fi
            if [[ "$pool_max_status" != "RED" && "$cap" -ge 40 ]]; then pool_max_status="YELLOW"; fi
            if [[ "$cap" -ge 80 ]]; then pool_max_status="RED"; fi
            if [[ "$cap" -ge 40 ]]; then pool_has_full="true"; fi
            
            # Bubble split-brain to pool
            if echo "$filesystems" | grep -E "^FILESYSTEM|$p_name(\||/)" | grep -q "|true|RED$"; then
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
            
            snap_parts=()
            [[ "$pool_has_sb" == "true" ]] && snap_parts+=("${C_RED}split-brain${C_RESET}")
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
        echo -e "${c_node}●${C_RESET} $n${n_desc}"
        [[ $node_reachable -ne 0 ]] && { echo -e "  ${C_RED}  [UNREACHABLE]${C_RESET}"; continue; }
        
        for p_name in $relevant_pools; do
            p_line=$(echo "$zpools" | awk -v p="$p_name" '$1 == p')
            health=$(echo "$p_line" | awk '{print $2}')
            cap=$(echo "$p_line" | awk '{print $3}' | tr -d '%')
            
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
            echo -e "  ${c_pool}💾${C_RESET} $p_name ($health, $cap%)${p_desc}"

            # Use process substitution or a read loop from a string to preserve state if needed,
            # but since we already evaluated colors, we just read them.
            while read -r ds_path; do
                [[ -z "$ds_path" ]] && continue
                ds_lines=$(echo "$filesystems" | grep "^FILESYSTEM|$ds_path|")
                
                # Evaluate dataset-level split-brain and status
                has_sb_ds="false"
                if echo "$ds_lines" | grep -q "|true|RED$"; then has_sb_ds="true"; fi
                
                ds_status="GREEN"
                [[ "$ds_lines" =~ \|RED$'\n' ]] && ds_status="RED"
                [[ "$ds_status" != "RED" && "$ds_lines" =~ \|YELLOW$'\n' ]] && ds_status="YELLOW"

                c_ds=$C_GREEN; [[ "$ds_status" == "YELLOW" ]] && c_ds=$C_YELLOW; [[ "$ds_status" == "RED" ]] && c_ds=$C_RED
                
                sb_label=""
                [[ "$has_sb_ds" == "true" ]] && sb_label=" ${C_RED}${C_BLINK}[split-brain]${C_RESET}"
                
                echo -e "    ${c_ds}📁${C_RESET} ${ds_path#$p_name/}${sb_label}"
                while read -r line; do
                    [[ -z "$line" ]] && continue
                    IFS='|' read -r _ _ label _ age conf hb has_sb_line c_logic <<< "$line"

                    age_str=$(format_minutes "$age")
                    label_desc=""
                    if [[ "$c_logic" == "YELLOW" ]]; then label_desc=" [late]"; fi
                    if [[ "$c_logic" == "RED" ]]; then label_desc=" [stale]"; fi
                    
                    c_label=$C_GREEN; [[ "$c_logic" == "YELLOW" ]] && c_label=$C_YELLOW; [[ "$c_logic" == "RED" ]] && c_label=$C_RED
                    [[ "$conf" == "false" ]] && echo -e "      - ${c_label}●${C_RESET} ${C_DIM}$label${C_RESET}: [${age_str}]${label_desc} ${C_RED}[unconfigured]${C_RESET}" || echo -e "      - ${c_label}●${C_RESET} $label: [${age_str}]${label_desc}"
                done <<< "$ds_lines"
            done < <(echo "$filesystems" | grep -E "^FILESYSTEM\|${p_name}(\||/)" | cut -d'|' -f2 | sort -u)
        done
    done
    
    return $global_exit_code
}
