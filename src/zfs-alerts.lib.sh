#!/bin/bash

# zfs-alerts.lib.sh - Notification functions for Zeplicator

send_smtp_alert() {
    local level=${1:-warn}
    local msg=$2
    local host=$(get_zfs_prop "zep:smtp_host" "$filesystem")
    local port=$(get_zfs_prop "zep:smtp_port" "$filesystem")
    local user=$(get_zfs_prop "zep:smtp_user" "$filesystem")
    local pass=$(get_zfs_prop "zep:smtp_password" "$filesystem")
    local from=$(get_zfs_prop "zep:smtp_from" "$filesystem")
    local to=$(get_zfs_prop "zep:smtp_to" "$filesystem")
    local proto=$(get_zfs_prop "zep:smtp_protocol" "$filesystem")
    
    [[ -z "$host" || -z "$to" ]] && return

    # --- Rate Limiting Logic ---
    local alias_val=${CLI_ALIAS:-$(hostname)}
    local prefix=$(get_snap_prefix "$filesystem")
    local state_dir="/tmp/${prefix}-${alias_val}-repl-alerts"
    mkdir -p "$state_dir"
    local ds_safe="${filesystem//\//-}"
    local state_file="${state_dir}/${ds_safe}.state"
    local msg_hash=$(echo -n "${level}:${msg}" | md5sum | awk '{print $1}')
    
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
    local threshold_str=$(get_zfs_prop "zep:alert:${level}:threshold" "$filesystem")
    local threshold=0
    if [[ "$threshold_str" != "-" ]]; then
        threshold=$(parse_time_to_seconds "$threshold_str")
    else
        case "$level" in
            critical) threshold=0 ;;
            warn)     threshold=3600 ;;
            info)     threshold=86400 ;;
        esac
    fi
    local elapsed=$((current_time - last_sent))

    if [[ $elapsed -lt $threshold && $last_sent -gt 0 ]]; then
        # Suppress and increment counter
        supp_count=$((supp_count + 1))
        # Update state file: remove old line, append new
        touch "$state_file"
        sed -i "/^$msg_hash /d" "$state_file"
        echo "$msg_hash $last_sent $supp_count" >> "$state_file"
        echo "    🔇 Alert suppressed (${level}: ${elapsed}s < ${threshold}s). Count: $supp_count"
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
    local err_log="/tmp/${prefix}-${alias_val}-replication.err"
    if [[ -f "$err_log" ]]; then
        detail=$(cat "$err_log")
        rm -f "$err_log"
    fi

    zbud_msg "  📧 Sending alert email to $to..."
    curl -s --url "${proto:-smtps}://${host}:${port:-465}" \
         --user "${user}:${pass}" \
         --mail-from "$from" \
         --mail-rcpt "$to" \
         --upload-file - <<EOF
From: $from
To: $to
Subject: ZFS Replication Alert: $filesystem on ${ME:-${my_hostname:-$(hostname)}}
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
