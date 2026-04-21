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
    
    local val=""
    
    # 1. Host-specific: repl:node:${ME}:keep:${lbl}
    val=$(get_zfs_prop "repl:node:${ME}:keep:${lbl}" "$ds")

    # 2. Role-specific: repl:role:<role>:keep:<label>
    [[ -z "$val" || "$val" == "-" ]] && val=$(get_zfs_prop "repl:role:${role}:keep:${lbl}" "$ds")

    # 3. Final Fallback
    [[ -z "$val" || "$val" == "-" ]] && val="$fallback"

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

    echo "${CHAIN_PREFIX}  🔄 Performing shipped-aware rotation for $ds (label: $lbl, keep: $k_count)..."
    
    # Get snapshots matching label, sorted by creation date (newest first)
    mapfile -t snaps < <(zfs list -t snap -H -o name,zfs-send:shipped -S creation -r "$ds" | grep "@.*$lbl")
    
    local count=${#snaps[@]}
    if [[ $count -le $k_count ]]; then
        echo "${CHAIN_PREFIX}  ✅ Snapshot count ($count) is within limit ($k_count). Skipping purge."
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
            echo "${CHAIN_PREFIX}  🗑️  Purging old shipped snapshot: $snap_name"
            zfs destroy "$snap_name"
        else
            echo "${CHAIN_PREFIX}  🛡️  KEEPING old snapshot (NOT YET SHIPPED): $snap_name"
        fi
    done
}
