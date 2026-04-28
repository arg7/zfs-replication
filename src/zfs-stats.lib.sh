# zfs-stats.lib.sh - Gather node state stats (called via `zep --stats`)
# Output format: ZPOOL:, IOSTAT:, FILESYSTEM|, TRANSFER|

cmd_stats() {
    local _alias="${CLI_ALIAS:-}"
    local _target_fs="${1:-}"
    [[ -z "$_alias" ]] && return 0

    local _node_role=""
    if [[ -z "$_target_fs" ]]; then
        # Fallback: scan datasets for chain membership (slow path)
        while read -r ds; do
            chain=$(zfs get -H -o value zep:chain "$ds" 2>/dev/null)
            [[ -z "$chain" || "$chain" == "-" ]] && continue
            if echo "$chain" | tr ',' '\n' | grep -qx "$_alias"; then
                _target_fs="${ds}"
                break
            fi
        done < <(zfs list -H -o name 2>/dev/null)
    fi
    [[ -z "$_target_fs" ]] && return 0

    # Determine role from chain position
    local chain=$(zfs get -H -o value zep:chain "$_target_fs" 2>/dev/null)
    if [[ -n "$chain" && "$chain" != "-" ]]; then
        IFS=',' read -r -a _nodes <<< "$chain"
        local _idx=0
        for _n in "${_nodes[@]}"; do
            if [[ "$_n" == "$_alias" ]]; then
                if [[ $_idx -eq 0 ]]; then _node_role="master"
                elif [[ $_idx -eq $((${#_nodes[@]}-1)) ]]; then _node_role="sink"
                else _node_role="middle"; fi
                break
            fi
            _idx=$((_idx+1))
        done
    fi

    # Get pool name from target filesystem
    local _pool="${_target_fs%%/*}"

    # 1. Zpools (only target pool)
    zpool list -H -o name,health,capacity "$_pool" 2>/dev/null | while read -r line; do
        echo "ZPOOL:$line"
    done

    # 2. Zpool IO stats (one-second sample, target pool)
    zpool iostat -H 1 1 2>/dev/null | grep "^${_pool}" | while read -r line; do
        echo "IOSTAT:$line"
    done

    # 3. Filesystems with zep: properties (under target FS)
    zfs list -H -o name -r "$_target_fs" 2>/dev/null | while read -r ds; do
        props=$(zfs get all -H -o property,value "$ds" 2>/dev/null | grep "^zep:")
        [[ -z "$props" ]] && continue

        prefix=$(echo "$props" | grep "zep:snap_prefix" | cut -f2)
        [[ -z "$prefix" || "$prefix" == "-" ]] && prefix="zep_"

        snap_list=$(zfs list -t snap -o name,creation -p -H -S creation -r "$ds" 2>/dev/null | grep "@$prefix")
        echo "$snap_list" | awk '{print $1}' | cut -d"@" -f2 | sed -E "s/^$prefix//" | cut -d"-" -f1 | sort -u | while read -r label; do
            [[ -z "$label" ]] && continue

            is_configured="false"
            heartbeat=$(echo "$props" | grep ":alert:heartbeat:${label}" | cut -f2)
            if echo "$props" | grep -q ":keep:${label}"; then
                is_configured="true"
            fi

            label_snaps=$(echo "$snap_list" | grep "@${prefix}${label}-" || true)
            snap_count=$(echo "$label_snaps" | grep -c "@${prefix}${label}-" 2>/dev/null || echo 0)

            # Prefer role-specific keep: zep:role:<_node_role>:keep:<label>
            keep_val=""
            [[ -n "$_node_role" ]] && keep_val=$(echo "$props" | grep "role:${_node_role}:keep:${label}" | head -n 1 | cut -f2)
            [[ -z "$keep_val" || "$keep_val" == "-" ]] && keep_val=$(echo "$props" | grep "role:.*:keep:${label}" | head -n 1 | cut -f2)
            [[ -z "$keep_val" || "$keep_val" == "-" ]] && keep_val=0

            if [[ -n "$label_snaps" ]]; then
                latest=$(echo "$label_snaps" | head -n 1)
                snap_name=$(echo "$latest" | awk '{print $1}')
                then=$(echo "$latest" | awk '{print $2}')
                now=$(date +%s)
                if [[ -n "$then" ]]; then
                    age=$(( (now - then) / 60 ))
                    has_sb=$(echo "$props" | grep ":error:split-brain" | cut -f2)
                    [[ "$has_sb" != "true" ]] && has_sb="false"
                    echo "FILESYSTEM|$ds|$label|$snap_name|$age|$is_configured|$heartbeat|$has_sb|$snap_count|$keep_val"
                fi
            fi
        done
    done

    # 4. Active ZFS transfer progress (from .err filenames, for this alias)
    # Filenames: <prefix>_<alias>-<ds-safe>-replication.err  e.g. zep_node1-zep-node-1-test-1-replication.err
    (
    for errfile in /tmp/*_${_alias}-*-replication.err; do
        [[ -f "$errfile" ]] || continue
        bname=$(basename "$errfile")
        ds_safe="${bname%-replication.err}"
        ds_safe="${ds_safe#*-}"
        total_actual=0
        for cnt_file in /tmp/*_${_alias}-${ds_safe}-*.lock.cnt; do
            [[ -f "$cnt_file" ]] || continue
            total_actual=$((total_actual + $(cat "$cnt_file" 2>/dev/null || echo 0)))
        done
        # File may be deleted by replication cleanup between -f check and read
        est_raw=$(grep -oP 'total estimated size is\s+\K.*' "$errfile" 2>/dev/null | xargs 2>/dev/null || echo 0)
        [[ -z "$est_raw" ]] && est_raw=0
        est=$(echo "$est_raw" | numfmt --from=auto 2>/dev/null | cut -d. -f1 || echo 0)
        if [[ "$est" -gt 0 || "$total_actual" -gt 0 ]]; then
            echo "TRANSFER|${ds_safe}|${est}|${total_actual}"
        fi
    done
    ) 2>/dev/null
}
