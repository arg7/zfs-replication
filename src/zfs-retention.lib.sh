#!/bin/bash

# zfs-retention.lib.sh - Snapshot rotation and retention functions for Zeplicator

# Resolve retention (keep count) for the current node
resolve_retention() {
    local ds=$1
    local lbl=$2
    local fallback=$3
    local role="middle"

    [[ $ME_INDEX -eq 0 ]] && role="master"
    [[ $ME_INDEX -eq $((${#nodes[@]} - 1)) ]] && role="sink"

    # Single batched zfs get to resolve all candidates at once
    local vals=$(zfs get -H -o value "zep:node:${ME}:keep:${lbl},zep:role:${role}:keep:${lbl}" "$ds" 2>/dev/null)
    local node_val=$(echo "$vals" | head -n 1)
    local role_val=$(echo "$vals" | tail -n 1)

    local val=""
    [[ -n "$node_val" && "$node_val" != "-" ]] && val="$node_val"
    [[ -z "$val" && -n "$role_val" && "$role_val" != "-" ]] && val="$role_val"
    [[ -z "$val" ]] && val="$fallback"

    # Ensure val is a number, otherwise default to a safe high number like 1000 or the fallback if numeric
    if [[ ! "$val" =~ ^[0-9]+$ ]]; then
       val=1000
    fi

    echo "$val"
}

    purge_shipped_snapshots() {
    local ds=$1
    local lbl=$2
    local k_count=$3

    # Defensive check for k_count being a number
    if [[ ! "$k_count" =~ ^[0-9]+$ ]]; then
        k_count=1000
    fi

    echo -e "${CHAIN_PREFIX}  ${C_YELLOW}🔄${C_RESET} Performing shipped-aware rotation for $ds (label: $lbl, keep: $k_count)..."
    
    local prefix=$(get_snap_prefix "$ds")
    # Get snapshots matching label, sorted by creation date (newest first)
    mapfile -t snaps < <(zfs list -t snap -H -o name,zep:shipped -S creation -r "$ds" | grep "@${prefix}${lbl}-")

    local count=${#snaps[@]}
    if [[ $count -le $k_count ]]; then
        echo -e "${CHAIN_PREFIX}  ${C_GREEN}✅${C_RESET} Snapshot count ($count) is within limit ($k_count). Skipping purge."
        return
    fi

    # Check if any snapshot in the KEEP range has shipped — if so, older unshipped snaps are safe to remove
    local newer_shipped=false
    for (( i=0; i<k_count; i++ )); do
        [[ "${snaps[i]}" == *"true"* ]] && { newer_shipped=true; break; }
    done

    local purged_count=0
    # Process snapshots from index k_count (0-indexed)
    for (( i=k_count; i<count; i++ )); do
        local line="${snaps[i]}"
        read -r snap_name shipped_val <<< "$line"

        if [[ "$shipped_val" == "true" ]]; then
            if [[ "$DRY_RUN" == true ]]; then
                echo -e "${CHAIN_PREFIX}  ${C_RED}🗑️${C_RESET}  [DRY RUN] Would purge old shipped snapshot: $snap_name"
            else
                echo -e "${CHAIN_PREFIX}  ${C_RED}🗑️${C_RESET}  Purging old shipped snapshot: $snap_name"
                zfs destroy "$snap_name"
                purged_count=$((purged_count + 1))
            fi
        elif [[ "$newer_shipped" == true ]]; then
            echo -e "${CHAIN_PREFIX}  ${C_RED}🗑️${C_RESET}  Purging old unshipped snapshot (newer shipped exists): $snap_name"
            zfs destroy "$snap_name"
            purged_count=$((purged_count + 1))
        else
            echo -e "${CHAIN_PREFIX}  ${C_BLUE}🛡️${C_RESET}  KEEPING old snapshot (NOT YET SHIPPED): $snap_name"
        fi
    done
    if [[ "$purged_count" -gt 0 ]]; then
        log_message "ROTATION: Purged $purged_count old shipped snapshot(s) from $ds (label: $lbl, kept: $k_count)"
    fi
}
