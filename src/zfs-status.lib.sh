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

        # 2. Datasets with zep: properties
        zfs list -H -o name -r "'$raw_ds'" 2>/dev/null | while read -r ds; do
            props=$(zfs get all -H -o property,value "$ds" 2>/dev/null | grep "^zep:")
            if [[ -n "$props" ]]; then
                # Find unique labels for this dataset
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
                            echo "DATASET|$ds|$label|$snap_name|$age|$is_configured|$heartbeat"
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
    local raw_dataset="$1"
    
    if [[ -z "$raw_dataset" ]]; then
        readarray -t datasets < <(zfs list -H -o name | while read ds; do if zfs get -H -o value zep:chain "$ds" 2>/dev/null | grep -qv "^-$"; then echo "$ds"; fi; done)
        [[ ${#datasets[@]} -eq 0 ]] && die "ERR: No datasets with zep:chain found."
        raw_dataset="${datasets[0]}"
    fi
    REPL_CHAIN=$(get_zfs_prop "zep:chain" "$raw_dataset")
    [[ -z "$REPL_CHAIN" ]] && die "ERR: No replication chain found."
    IFS=',' read -r -a nodes <<< "$REPL_CHAIN"
    
    local global_exit_code=0

    for n in "${nodes[@]}"; do
        node_ds=$(resolve_node_dataset "$n" "$raw_dataset")
        out=$(get_node_state "$n" "$node_ds")
        
        node_reachable=$?
        
        zpools=$(echo "$out" | grep "^ZPOOL:" | cut -d':' -f2-)
        datasets_raw=$(echo "$out" | grep "^DATASET|")
        
        # Pre-evaluate dataset colors to allow hierarchical bubbling
        datasets=""
        while read -r line; do
            [[ -z "$line" ]] && continue
            IFS='|' read -r _ _ label _ age conf hb <<< "$line"
            
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
            
            c=$C_GREEN; [[ $age -ge $((hb*5)) ]] && c=$C_YELLOW; [[ $age -ge $((hb*10)) ]] && c=$C_RED
            datasets+="${line}|${c}"$'\n'
        done <<< "$datasets_raw"

        relevant_pools=$(echo "$datasets" | cut -d'|' -f2 | cut -d'/' -f1 | sort -u)
        
        # Pre-evaluate Zpool statuses
        pool_max_status=$C_GREEN
        for p_name in $relevant_pools; do
            p_line=$(echo "$zpools" | awk -v p="$p_name" '$1 == p')
            health=$(echo "$p_line" | awk '{print $2}')
            cap=$(echo "$p_line" | awk '{print $3}' | tr -d '%')
            
            [[ "$health" != "ONLINE" ]] && pool_max_status=$C_RED
            [[ "$pool_max_status" != "$C_RED" && "$cap" -ge 40 ]] && pool_max_status=$C_YELLOW
            [[ "$cap" -ge 80 ]] && pool_max_status=$C_RED
        done

        # Aggregate Node status
        n_status=$C_GREEN
        [[ $node_reachable -ne 0 ]] && n_status=$C_RED
        [[ "$n_status" != "$C_RED" && "$datasets" =~ "$C_RED" ]] && n_status=$C_RED
        [[ "$n_status" != "$C_RED" && "$datasets" =~ "$C_YELLOW" ]] && n_status=$C_YELLOW
        [[ "$n_status" != "$C_RED" && "$pool_max_status" == "$C_RED" ]] && n_status=$C_RED
        [[ "$n_status" != "$C_RED" && "$pool_max_status" == "$C_YELLOW" ]] && n_status=$C_YELLOW
        
        # Update global exit code
        if [[ "$n_status" == "$C_RED" ]]; then
            global_exit_code=2
        elif [[ "$n_status" == "$C_YELLOW" && $global_exit_code -lt 1 ]]; then
            global_exit_code=1
        fi
        
        echo -e "${n_status}●${C_RESET} $n"
        [[ $node_reachable -ne 0 ]] && { echo -e "  ${C_RED}  [UNREACHABLE]${C_RESET}"; continue; }
        
        for p_name in $relevant_pools; do
            p_line=$(echo "$zpools" | awk -v p="$p_name" '$1 == p')
            health=$(echo "$p_line" | awk '{print $2}')
            cap=$(echo "$p_line" | awk '{print $3}' | tr -d '%')
            
            p_status=$C_GREEN
            [[ "$health" != "ONLINE" ]] && p_status=$C_RED
            [[ "$cap" -ge 40 ]] && p_status=$C_YELLOW
            [[ "$cap" -ge 80 ]] && p_status=$C_RED
            
            # Aggregate from child datasets
            ds_statuses=$(echo "$datasets" | grep "^DATASET|$p_name")
            [[ "$p_status" != "$C_RED" && "$ds_statuses" =~ "$C_RED" ]] && p_status=$C_RED
            [[ "$p_status" != "$C_RED" && "$ds_statuses" =~ "$C_YELLOW" ]] && p_status=$C_YELLOW

            echo -e "  ${p_status}💾${C_RESET} $p_name ($health, $cap%)"

            # Use process substitution or a read loop from a string to preserve state if needed,
            # but since we already evaluated colors, we just read them.
            while read -r ds; do
                [[ -z "$ds" ]] && continue
                ds_lines=$(echo "$datasets" | grep "^DATASET|$ds|")
                ds_status=$C_GREEN
                [[ "$ds_lines" =~ "$C_RED" ]] && ds_status=$C_RED
                [[ "$ds_status" != "$C_RED" && "$ds_lines" =~ "$C_YELLOW" ]] && ds_status=$C_YELLOW

                echo -e "    ${ds_status}📁${C_RESET} $(basename "$ds")"
                while read -r line; do
                    [[ -z "$line" ]] && continue
                    IFS='|' read -r _ _ label _ age conf hb c <<< "$line"

                    age_str=$(format_minutes "$age")
                    [[ "$conf" == "false" ]] && echo -e "      - ${C_DIM}$label${C_RESET}: [$c${age_str}${C_RESET}] ${C_RED}[unconfigured]${C_RESET}" || echo -e "      - $label: [$c${age_str}${C_RESET}]"
                done <<< "$ds_lines"
            done < <(echo "$datasets" | grep -E "^DATASET\|${p_name}(\||/)" | cut -d'|' -f2 | sort -u)
        done
    done
    
    return $global_exit_code
}
