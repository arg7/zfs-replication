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
    # Include GUID in output for last_snapshot protection check
    mapfile -t snapshots < <(zfs list -t snapshot -H -o name,zep:shipped,guid -S creation -r "$ds" | grep "@${prefix}${lbl}-")

    local count=${#snapshots[@]}
    if [[ $count -le $k_count ]]; then
        echo -e "${CHAIN_PREFIX}  ${C_GREEN}✅${C_RESET} Snapshot count ($count) is within limit ($k_count). Skipping purge."
        return
    fi

    # Collect protected GUIDs from per-node last_snapshot properties.
    # When a node goes offline, its last_snapshot entry stays frozen;
    # every node in the chain must preserve these snapshots as common ground.
    local -A protected_guids
    local -A guid_to_node
    while IFS= read -r prop_line; do
        [[ -z "$prop_line" ]] && continue
        local p_node="${prop_line%:last_snapshot*}"
        p_node="${p_node##*:node:}"
        local p_guid="${prop_line##*$'\t'}"
        [[ -n "$p_guid" && "$p_guid" != "-" ]] && protected_guids["$p_guid"]=1 && guid_to_node["$p_guid"]="$p_node"
    done < <(zfs get all -H -o property,value "$ds" 2>/dev/null | grep "^zep:node:" | grep "last_snapshot")

    if [[ ${#protected_guids[@]} -gt 0 ]]; then
        echo -e "${CHAIN_PREFIX}  ${C_DIM}ℹ️${C_RESET}  Protecting ${#protected_guids[@]} snapshot(s) as per-node last_snapshot."
    fi

    # Check if any snapshot in the KEEP range has shipped — if so, older unshipped snaps are safe to remove
    local newer_shipped=false
    for (( i=0; i<k_count; i++ )); do
        [[ "${snapshots[i]}" == *"true"* ]] && { newer_shipped=true; break; }
    done

    local purged_count=0
    local kept_last_shipped=false
    local alerted_protected=false
    # Process snapshots from index k_count (0-indexed), newest first within purge range
    for (( i=k_count; i<count; i++ )); do
        local line="${snapshots[i]}"
        read -r snap_name shipped_val snap_guid <<< "$line"

        # Never purge a snapshot that is a per-node last_snapshot (common ground)
        if [[ -n "${protected_guids[$snap_guid]+x}" ]]; then
            if [[ "$alerted_protected" != "true" ]]; then
                local _saved_fs="${filesystem:-}"
                filesystem="$ds"
                send_smtp_alert "warning" --task "rotation" --status "last_snapshot at risk" "WARNING: Common-ground snapshot for node ${guid_to_node[$snap_guid]} is outside retention window on $ds (label: $lbl, keep: $k_count). Consider increasing keep count."
                [[ -n "$_saved_fs" ]] && filesystem="$_saved_fs" || unset filesystem
                alerted_protected=true
            fi
            echo -e "${CHAIN_PREFIX}  ${C_BLUE}🛡️${C_RESET}  KEEPING protected last_snapshot: $snap_name (GUID: $snap_guid)"
            kept_last_shipped=true
            newer_shipped=true
            continue
        fi

        if [[ "$shipped_val" == "true" ]]; then
            if [[ "$kept_last_shipped" == false ]]; then
                echo -e "${CHAIN_PREFIX}  ${C_BLUE}🛡️${C_RESET}  KEEPING last shipped snapshot (common ground): $snap_name"
                kept_last_shipped=true
                newer_shipped=true
            elif [[ "$DRY_RUN" == true ]]; then
                echo -e "${CHAIN_PREFIX}  ${C_RED}🗑️${C_RESET}  [DRY RUN] Would purge old shipped snapshot: $snap_name"
            else
                echo -e "${CHAIN_PREFIX}  ${C_RED}🗑️${C_RESET}  Purging old shipped snapshot: $snap_name"
                zfs destroy "$snap_name" 2>/dev/null || {
                    echo -e "${CHAIN_PREFIX}  ${C_BLUE}🛡️${C_RESET}  Cannot destroy $snap_name (clone origin?), keeping for safety."
                    continue
                }
                purged_count=$((purged_count + 1))
            fi
        elif [[ "$newer_shipped" == true ]]; then
            echo -e "${CHAIN_PREFIX}  ${C_RED}🗑️${C_RESET}  Purging old unshipped snapshot (newer shipped exists): $snap_name"
            zfs destroy "$snap_name" 2>/dev/null || true
            purged_count=$((purged_count + 1))
        else
            echo -e "${CHAIN_PREFIX}  ${C_BLUE}🛡️${C_RESET}  KEEPING old snapshot (NOT YET SHIPPED): $snap_name"
        fi
    done
    if [[ "$purged_count" -gt 0 ]]; then
        log_message "ROTATION: Purged $purged_count old shipped snapshot(s) from $ds (label: $lbl, kept: $k_count)"
    fi
}
