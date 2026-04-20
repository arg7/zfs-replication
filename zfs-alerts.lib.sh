#!/bin/bash

# zfs-alerts.lib.sh - Notification functions for Zeplicator

send_smtp_alert() {
    local msg=$1
    local host=$(get_zfs_prop "repl:smtp_host" "$dataset")
    local port=$(get_zfs_prop "repl:smtp_port" "$dataset")
    local user=$(get_zfs_prop "repl:smtp_user" "$dataset")
    local pass=$(get_zfs_prop "repl:smtp_password" "$dataset")
    local from=$(get_zfs_prop "repl:smtp_from" "$dataset")
    local to=$(get_zfs_prop "repl:smtp_to" "$dataset")
    local proto=$(get_zfs_prop "repl:smtp_protocol" "$dataset")
    
    [[ -z "$host" || -z "$to" ]] && return

    # --- Rate Limiting Logic ---
    local state_dir="/tmp/zfs-repl-alerts"
    mkdir -p "$state_dir"
    # Use a safe filename for the dataset state
    local ds_safe="${dataset//\//-}"
    local state_file="${state_dir}/${ds_safe}.state"
    # Hash the core message (excluding dynamic error details to group similar errors)
    local msg_hash=$(echo -n "$msg" | md5sum | awk '{print $1}')
    
    local last_sent=0
    local supp_count=0
    if [[ -f "$state_file" ]]; then
        local line=$(grep "^$msg_hash " "$state_file")
        if [[ -n "$line" ]]; then
            last_sent=$(echo "$line" | awk '{print $2}')
            supp_count=$(echo "$line" | awk '{print $3}')
        fi
    fi

    local current_time=$(date +%s)
    local threshold=1800 # 30 minutes
    local elapsed=$((current_time - last_sent))

    if [[ $elapsed -lt $threshold && $last_sent -gt 0 ]]; then
        # Suppress and increment counter
        supp_count=$((supp_count + 1))
        # Update state file: remove old line, append new
        touch "$state_file"
        sed -i "/^$msg_hash /d" "$state_file"
        echo "$msg_hash $last_sent $supp_count" >> "$state_file"
        echo "    🔇 Alert suppressed (Rate Limit: ${elapsed}s < ${threshold}s). Count: $supp_count"
        return
    fi

    # Prepare summary for suppressed alerts
    local rate_limit_notice=""
    if [[ $supp_count -gt 0 ]]; then
        rate_limit_notice="[Note: In the last $((elapsed / 60)) minutes, this specific alert was repeated and suppressed $supp_count times.]"
    fi
    # --- End Rate Limiting Logic ---

    # Include captured error details if they exist
    local detail=""
    if [[ -f "/tmp/zfs-replication.err" ]]; then
        detail=$(cat /tmp/zfs-replication.err)
        rm -f /tmp/zfs-replication.err
    fi

    echo "  📧 Sending alert email to $to..."
    curl -s --url "${proto:-smtps}://${host}:${port:-465}" \
         --user "${user}:${pass}" \
         --mail-from "$from" \
         --mail-rcpt "$to" \
         --upload-file - <<EOF
From: $from
To: $to
Subject: ZFS Replication Alert: $dataset on ${ME:-${my_hostname:-$(hostname)}}
Date: $(date -R)

$msg
$rate_limit_notice

--- Error Details ---
${detail:-No specific error details captured.}
EOF

    # Update state file with new sent time and reset counter
    touch "$state_file"
    sed -i "/^$msg_hash /d" "$state_file"
    echo "$msg_hash $current_time 0" >> "$state_file"
}
