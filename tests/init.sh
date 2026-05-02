#!/bin/bash
set -e

# Load configuration
SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
CONF_FILE="$SCRIPT_DIR/test.conf"

if [[ -f "$CONF_FILE" ]]; then
    source "$CONF_FILE"
else
    echo "Error: $CONF_FILE not found."
    exit 1
fi

NUM_NODES=${NUM_NODES:-3}
ZEP_BIN=$(command -v zep)
ZEP_SCRIPT="${SCRIPT_DIR}/../build/zep"

# ── mode detection ────────────────────────────────────────
# DISTRIBUTED=true   → nodes are real machines, root SSH to set up
# DISTRIBUTED=false  → simulated: all nodes on same machine, ramdisk
DISTRIBUTED=${DISTRIBUTED:-false}
RAMDISK="/tmp/zep-ramdisk"

# Detect ZFS type
ZFS_FUSE=false
[[ -d /sys/module/zfs ]] || ZFS_FUSE=true

# ── dependency check ──────────────────────────────────────
SYSTEM_DEPS=("zfs" "zpool" "ssh" "mbuffer" "zstd" "curl" "tmux")
PROJECT_DEPS=("zep" "iomon")
MISSING_SYSTEM=()
MISSING_PROJECT=()

for dep in "${SYSTEM_DEPS[@]}"; do
    command -v "$dep" >/dev/null 2>&1 || MISSING_SYSTEM+=("$dep")
done
for dep in "${PROJECT_DEPS[@]}"; do
    command -v "$dep" >/dev/null 2>&1 || MISSING_PROJECT+=("$dep")
done

if [[ ${#MISSING_SYSTEM[@]} -ne 0 ]] || [[ ${#MISSING_PROJECT[@]} -ne 0 ]]; then
    if [[ ${#MISSING_SYSTEM[@]} -ne 0 ]]; then
        echo "Error: Missing system dependencies: ${MISSING_SYSTEM[*]}"
        echo "Install: sudo apt install zfsutils-linux openssh-client mbuffer zstd curl tmux"
    fi
    if [[ ${#MISSING_PROJECT[@]} -ne 0 ]]; then
        echo "Error: Missing project binaries: ${MISSING_PROJECT[*]}"
        echo "Run: sudo make install inside $SCRIPT_DIR/.."
    fi
    exit 1
fi

# ── node config resolvers ─────────────────────────────────
# Reads test.conf vars: NODE<n>_FQDN, NODE<n>_POOL, NODE<n>_DS, NODE<n>_USER
# Falls back to convention: node-$i, zep-node-$i, zep-node-$i/test-$i, zep-user-$i

get_node_fqdn() {
    local i="$1"
    local var="NODE${i}_FQDN"
    echo "${!var:-zep-node-${i}.local}"
}

get_node_pool() {
    local i="$1"
    local var="NODE${i}_POOL"
    echo "${!var:-zep-node-${i}}"
}

get_node_ds() {
    local i="$1"
    local var="NODE${i}_DS"
    echo "${!var:-zep-node-${i}/test-${i}}"
}

get_node_user() {
    local i="$1"
    local var="NODE${i}_USER"
    echo "${!var:-zep-user-${i}}"
}

get_node_device() {
    local i="$1"
    local var="NODE${i}_POOL_DEVICE"
    echo "${!var:-}"
}

# ── root SSH wrapper for remote nodes ─────────────────────
# Requires: current user can passwordless SSH as root to remote hosts
_root_ssh() {
    local fqdn="$1" cmd="$2"
    ssh -n -o StrictHostKeyChecking=no "root@${fqdn}" "$cmd"
}

# ── distributed: setup a remote node ─────────────────────
_setup_remote_node() {
    local i="$1"
    local fqdn pool ds user dev
    fqdn=$(get_node_fqdn "$i")
    pool=$(get_node_pool "$i")
    ds=$(get_node_ds "$i")
    user=$(get_node_user "$i")
    dev=$(get_node_device "$i")

    echo "── [node$i] $fqdn ──────────────────────────────────"

    # 1. Create zep-user if not exists
    echo "  Creating user $user on $fqdn..."
    _root_ssh "$fqdn" "
        if ! id '$user' >/dev/null 2>&1; then
            useradd -m -s /bin/bash '$user' 2>/dev/null || \
            adduser --disabled-password --gecos '' '$user' 2>/dev/null || true
        fi
    "

    # 2. Set up SSH key for zep-user
    _root_ssh "$fqdn" "
        ZEP_HOME=\$(getent passwd '$user' | cut -d: -f6)
        [[ -z \"\$ZEP_HOME\" ]] && ZEP_HOME='/home/$user'
        mkdir -p \"\$ZEP_HOME/.ssh\"
        if [[ ! -f \"\$ZEP_HOME/.ssh/id_rsa\" ]]; then
            ssh-keygen -t rsa -b 2048 -f \"\$ZEP_HOME/.ssh/id_rsa\" -N '' -q
        fi
        chown -R '$user:$user' \"\$ZEP_HOME/.ssh\"
        chmod 700 \"\$ZEP_HOME/.ssh\"
    "

    # 3. Create ZFS pool if device given and pool doesn't exist
    if [[ -n "$dev" ]]; then
        if _root_ssh "$fqdn" "zpool list '$pool' >/dev/null 2>&1"; then
            echo "  Pool $pool already exists on $fqdn"
        else
            echo "  Creating pool $pool on $dev at $fqdn..."
            _root_ssh "$fqdn" "zpool create '$pool' '$dev'"
        fi
    else
        if ! _root_ssh "$fqdn" "zpool list '$pool' >/dev/null 2>&1"; then
            echo "  ${C_RED}ERROR:${C_RESET} Pool $pool not found on $fqdn and no NODE${i}_POOL_DEVICE set."
            return 1
        fi
        echo "  Pool $pool exists on $fqdn (pre-existing)"
    fi

    # 4. Create dataset and set properties
    if ! _root_ssh "$fqdn" "zfs list '$ds' >/dev/null 2>&1"; then
        echo "  Creating dataset $ds..."
        _root_ssh "$fqdn" "zfs create '$ds'"
    fi
    if [[ $i -ne 1 ]]; then
        _root_ssh "$fqdn" "
            zfs set canmount=noauto '$ds' 2>/dev/null || true
            zfs unmount '$ds' 2>/dev/null || true
        "
    fi

    # 5. Delegate ZFS permissions
    echo "  Delegating ZFS permissions to $user..."
    _root_ssh "$fqdn" "zfs allow '$user' create,mount,receive,destroy,userprop,diff '$pool'"
    _root_ssh "$fqdn" "zfs allow '$user' create,destroy,send,receive,snapshot,hold,release,userprop '$ds'"

    echo "  ✅ node$i ($fqdn) ready"
}

# ── distributed: setup master node (node1, local) ─────────
_setup_local_node() {
    local i=1
    local pool ds user dev
    pool=$(get_node_pool "$i")
    ds=$(get_node_ds "$i")
    user=$(get_node_user "$i")
    dev=$(get_node_device "$i")

    echo "── [node$i] local (master) ─────────────────────"

    # Create user if needed
    if ! id "$user" >/dev/null 2>&1; then
        echo "  Creating local user $user..."
        useradd -m -s /bin/bash "$user" 2>/dev/null || \
        adduser --disabled-password --gecos "" "$user" 2>/dev/null || true
    fi

    # SSH key for zep-user
    local ZEP_HOME
    ZEP_HOME=$(getent passwd "$user" | cut -d: -f6)
    [[ -z "$ZEP_HOME" ]] && ZEP_HOME="/home/$user"
    local ZEP_SSH_DIR="$ZEP_HOME/.ssh"
    mkdir -p "$ZEP_SSH_DIR"
    if [[ ! -f "$ZEP_SSH_DIR/id_rsa" ]]; then
        ssh-keygen -t rsa -b 2048 -f "$ZEP_SSH_DIR/id_rsa" -N "" -q
    fi
    chown -R "$user:$user" "$ZEP_SSH_DIR"
    chmod 700 "$ZEP_SSH_DIR"

    # Create pool if device given
    if [[ -n "$dev" ]]; then
        if zpool list "$pool" >/dev/null 2>&1; then
            echo "  Pool $pool already exists locally"
        else
            echo "  Creating local pool $pool on $dev..."
            zpool create "$pool" "$dev"
        fi
    else
        if ! zpool list "$pool" >/dev/null 2>&1; then
            echo "  ${C_RED}ERROR:${C_RESET} Local pool $pool not found and no NODE1_POOL_DEVICE set."
            echo "  Set NODE1_POOL_DEVICE in test.conf or create the pool manually."
            return 1
        fi
        echo "  Pool $pool exists locally (pre-existing)"
    fi

    # Create dataset if needed
    if ! zfs list "$ds" >/dev/null 2>&1; then
        echo "  Creating local dataset $ds..."
        zfs create "$ds"
    fi

    # Delegate ZFS permissions
    echo "  Delegating local ZFS permissions to $user..."
    zfs allow "$user" create,mount,receive,destroy,userprop,diff "$pool"
    zfs allow "$user" create,destroy,send,receive,snapshot,hold,release,userprop "$ds"

    echo "  ✅ node1 (local) ready"
}

# ── distributed: SSH mesh key distribution ────────────────
_setup_distributed_mesh() {
    echo "── SSH mesh key distribution ────────────────────"

    # Collect all zep-user pubkeys
    local KEYS_DIR="/tmp/zep-init-keys"
    rm -rf "$KEYS_DIR" 2>/dev/null || true
    mkdir -p "$KEYS_DIR"

    for i in $(seq 1 "$NUM_NODES"); do
        local fqdn user
        fqdn=$(get_node_fqdn "$i")
        user=$(get_node_user "$i")
        if [[ $i -eq 1 ]]; then
            local ZEP_HOME
            ZEP_HOME=$(getent passwd "$user" | cut -d: -f6)
            [[ -z "$ZEP_HOME" ]] && ZEP_HOME="/home/$user"
            cp "$ZEP_HOME/.ssh/id_rsa.pub" "$KEYS_DIR/node${i}.pub"
        else
            _root_ssh "$fqdn" "
                ZEP_HOME=\$(getent passwd '$user' | cut -d: -f6)
                [[ -z \"\$ZEP_HOME\" ]] && ZEP_HOME='/home/$user'
                cat \"\$ZEP_HOME/.ssh/id_rsa.pub\"
            " > "$KEYS_DIR/node${i}.pub"
        fi
        echo "  Collected pubkey for $user@$fqdn"
    done

    # Add current user's pubkey to mesh (so test runner can SSH as zep-user)
    local user_pubkey=""
    for kt in id_ed25519 id_rsa id_ecdsa; do
        if [[ -f "${HOME}/.ssh/${kt}.pub" ]]; then
            user_pubkey="${HOME}/.ssh/${kt}.pub"
            break
        fi
    done
    [[ -n "$user_pubkey" ]] && cp "$user_pubkey" "$KEYS_DIR/runner.pub"

    # Distribute all pubkeys to every node
    for i in $(seq 1 "$NUM_NODES"); do
        local fqdn user
        fqdn=$(get_node_fqdn "$i")
        user=$(get_node_user "$i")

        # Combine all pubkeys that ARE NOT this node's own key (plus runner key)
        local tmpkeys="$KEYS_DIR/combined-${i}.pub"
        > "$tmpkeys"
        for j in $(seq 1 "$NUM_NODES"); do
            [[ $i -eq $j ]] && continue
            cat "$KEYS_DIR/node${j}.pub" >> "$tmpkeys"
        done
        [[ -f "$KEYS_DIR/runner.pub" ]] && cat "$KEYS_DIR/runner.pub" >> "$tmpkeys"
        sort -u "$tmpkeys" -o "$tmpkeys"

        if [[ $i -eq 1 ]]; then
            # Local: append to zep-user's authorized_keys
            local ZEP_HOME
            ZEP_HOME=$(getent passwd "$user" | cut -d: -f6)
            [[ -z "$ZEP_HOME" ]] && ZEP_HOME="/home/$user"
            cat "$tmpkeys" > "$ZEP_HOME/.ssh/authorized_keys"
            chown "$user:$user" "$ZEP_HOME/.ssh/authorized_keys"
            chmod 600 "$ZEP_HOME/.ssh/authorized_keys"
        else
            # Remote: pipe keys via SSH
            cat "$tmpkeys" | _root_ssh "$fqdn" "
                ZEP_HOME=\$(getent passwd '$user' | cut -d: -f6)
                [[ -z \"\$ZEP_HOME\" ]] && ZEP_HOME='/home/$user'
                cat > \"\$ZEP_HOME/.ssh/authorized_keys\"
                chown '$user:$user' \"\$ZEP_HOME/.ssh/authorized_keys\"
                chmod 600 \"\$ZEP_HOME/.ssh/authorized_keys\"
            "
        fi
        echo "  ✅ Authorized keys on $user@$fqdn"
    done

    # Also add all zep-user keys to root's authorized_keys (for debugging)
    local root_keys="$KEYS_DIR/root-combined.pub"
    > "$root_keys"
    for i in $(seq 1 "$NUM_NODES"); do
        cat "$KEYS_DIR/node${i}.pub" >> "$root_keys"
    done
    sort -u "$root_keys" -o "$root_keys"
    cat "$root_keys" >> /root/.ssh/authorized_keys 2>/dev/null || true
    sort -u /root/.ssh/authorized_keys -o /root/.ssh/authorized_keys 2>/dev/null || true

    # Propagate known_hosts
    for i in $(seq 1 "$NUM_NODES"); do
        local fqdn user
        fqdn=$(get_node_fqdn "$i")
        user=$(get_node_user "$i")
        if [[ $i -ne 1 ]]; then
            if [[ -f /root/.ssh/known_hosts ]]; then
                cat /root/.ssh/known_hosts | _root_ssh "$fqdn" "
                    ZEP_HOME=\$(getent passwd '$user' | cut -d: -f6)
                    [[ -z \"\$ZEP_HOME\" ]] && ZEP_HOME='/home/$user'
                    cat >> \"\$ZEP_HOME/.ssh/known_hosts\"
                    chown '$user:$user' \"\$ZEP_HOME/.ssh/known_hosts\"
                    chmod 600 \"\$ZEP_HOME/.ssh/known_hosts\"
                " 2>/dev/null || true
            fi
        fi
    done

    rm -rf "$KEYS_DIR" 2>/dev/null || true
    echo "  ✅ SSH mesh complete"
}

# ── distributed: install cron on all nodes ────────────────
_setup_distributed_cron() {
    echo "── Installing cron for snapshot rotation ───────"

    for i in $(seq 1 "$NUM_NODES"); do
        local fqdn user ds
        fqdn=$(get_node_fqdn "$i")
        ds=$(get_node_ds "$i")

        local cron_line="* * * * * $ZEP_BIN --alias node$i --rotate $ds >/dev/null 2>&1"
        if [[ $i -eq 1 ]]; then
            (crontab -l 2>/dev/null | grep -v 'zep.*--rotate' || true
             echo "$cron_line") | crontab -
        else
            _root_ssh "$fqdn" "
                (crontab -l 2>/dev/null | grep -v 'zep.*--rotate' || true
                 echo '$cron_line') | crontab -
                systemctl restart crond 2>/dev/null || service crond restart 2>/dev/null || true
            "
        fi
        echo "  ✅ Cron on node$i ($fqdn)"
    done
    systemctl restart crond 2>/dev/null || service crond restart 2>/dev/null || true
}

# ── distributed: /etc/hosts with real IPs ─────────────────
_setup_distributed_hosts() {
    echo "── Configuring /etc/hosts ──────────────────────"
    for i in $(seq 1 "$NUM_NODES"); do
        local fqdn ip
        fqdn=$(get_node_fqdn "$i")
        # Resolve to IP for /etc/hosts (in case FQDN is an IP already)
        ip=$(getent hosts "$fqdn" 2>/dev/null | awk '{print $1}' | head -1)
        if [[ -z "$ip" ]]; then
            ip="$fqdn"  # assume it's already an IP
        fi
        sed -i "/zep-node-${i}\.local/d" /etc/hosts
        echo "$ip $fqdn zep-node-${i}.local" >> /etc/hosts
        echo "  Added: $ip $fqdn"
    done
}

# ── simulated: ramdisk + local pools ──────────────────────
_setup_sim_ramdisk() {
    local POOL_SIZE=${POOL_SIZE:-128M}
    local pool_bytes ramdisk_size
    pool_bytes=$(numfmt --from=iec "$POOL_SIZE")
    ramdisk_size=$(( pool_bytes * NUM_NODES + pool_bytes / 2 ))

    if mountpoint -q "$RAMDISK" 2>/dev/null; then
        echo "Ramdisk already mounted at $RAMDISK"
    else
        echo "Creating ${ramdisk_size}-byte ramdisk at $RAMDISK..."
        mkdir -p "$RAMDISK"
        mount -t tmpfs -o size="${ramdisk_size}" tmpfs "$RAMDISK"
        echo "Ramdisk mounted at $RAMDISK"
    fi
    rm -rf /tmp/zep_* 2>/dev/null || true
}

_setup_sim_pool() {
    local i="$1"
    local pool ds img user
    pool=$(get_node_pool "$i")
    ds=$(get_node_ds "$i")
    user=$(get_node_user "$i")
    img="$RAMDISK/$pool.img"

    echo "Setting up $pool..."

    truncate -s "$POOL_SIZE" "$img"
    zpool destroy -f "$pool" 2>/dev/null || true
    zpool labelclear -f "$img" 2>/dev/null || true
    zpool create "$pool" "$img"
    zfs create "$ds"
    if [[ $i -ne 1 ]]; then
        zfs set canmount=noauto "$ds"
        zfs unmount "$ds" 2>/dev/null || true
    fi
    echo "  Created $ds on $pool"
}

_setup_sim_user() {
    local i="$1"
    local pool ds user
    pool=$(get_node_pool "$i")
    ds=$(get_node_ds "$i")
    user=$(get_node_user "$i")

    echo "  Setting up $user with minimal ZFS rights..."

    if ! id "$user" >/dev/null 2>&1; then
        useradd -m -s /bin/bash "$user" 2>/dev/null || \
        adduser --disabled-password --gecos "" "$user" 2>/dev/null || true
    fi

    local ZEP_HOME
    ZEP_HOME=$(getent passwd "$user" | cut -d: -f6)
    [[ -z "$ZEP_HOME" ]] && ZEP_HOME="/home/$user"
    local ZEP_SSH_DIR="$ZEP_HOME/.ssh"
    mkdir -p "$ZEP_SSH_DIR"

    if [[ ! -f "$ZEP_SSH_DIR/id_rsa" ]]; then
        ssh-keygen -t rsa -b 2048 -f "$ZEP_SSH_DIR/id_rsa" -N "" -q
    fi

    # Add current user's pubkey to zep-user's authorized_keys
    local user_pubkey=""
    for kt in id_ed25519 id_rsa id_ecdsa; do
        if [[ -f "${HOME}/.ssh/${kt}.pub" ]]; then
            user_pubkey="${HOME}/.ssh/${kt}.pub"
            break
        fi
    done
    if [[ -n "$user_pubkey" ]]; then
        cp "$user_pubkey" "$ZEP_SSH_DIR/authorized_keys"
    else
        if [[ ! -f "${HOME}/.ssh/id_rsa" ]]; then
            mkdir -p "${HOME}/.ssh"
            ssh-keygen -t rsa -b 2048 -f "${HOME}/.ssh/id_rsa" -N "" -q
        fi
        cat "${HOME}/.ssh/id_rsa.pub" > "$ZEP_SSH_DIR/authorized_keys"
    fi

    chown -R "$user:$user" "$ZEP_SSH_DIR"
    chmod 700 "$ZEP_SSH_DIR"
    chmod 600 "$ZEP_SSH_DIR/id_rsa" "$ZEP_SSH_DIR/authorized_keys"
    chmod 644 "$ZEP_SSH_DIR/id_rsa.pub" 2>/dev/null || true

    # Delegate ZFS permissions (zfs-fuse skips — no delegation model)
    if [[ "$ZFS_FUSE" == "true" ]]; then
        echo "  ℹ️  zfs-fuse detected, skipping zfs allow delegation"
    else
        zfs allow "$user" create,mount,receive,destroy,userprop,diff "$pool" 2>/dev/null || \
            echo "  ⚠️  pool-level delegation failed (may need root)"
        zfs allow "$user" create,destroy,send,receive,snapshot,hold,release,userprop "$ds" 2>/dev/null || \
            echo "  ⚠️  dataset-level delegation failed (may need root)"
    fi

    echo "  ✅ $user set up with delegated ZFS rights on $pool"
}

_setup_sim_mesh() {
    echo "Setting up full-mesh SSH auth between zep-users..."
    for i in $(seq 1 "$NUM_NODES"); do
        local user_i="$(get_node_user "$i")"
        local ZEP_HOME_I
        ZEP_HOME_I=$(getent passwd "$user_i" | cut -d: -f6)
        [[ -z "$ZEP_HOME_I" ]] && ZEP_HOME_I="/home/$user_i"

        for j in $(seq 1 "$NUM_NODES"); do
            [[ $i -eq $j ]] && continue
            local user_j="$(get_node_user "$j")"
            local JHOME
            JHOME=$(getent passwd "$user_j" | cut -d: -f6)
            [[ -z "$JHOME" ]] && JHOME="/home/$user_j"
            cat "$JHOME/.ssh/id_rsa.pub" >> "$ZEP_HOME_I/.ssh/authorized_keys" 2>/dev/null || true
        done
        # Also add zep-user's pubkey to root's authorized_keys
        cat "$ZEP_HOME_I/.ssh/id_rsa.pub" >> /root/.ssh/authorized_keys 2>/dev/null || true
    done

    echo "Deduplicating authorized_keys..."
    for i in $(seq 1 "$NUM_NODES"); do
        local user="$(get_node_user "$i")"
        local ZEP_HOME
        ZEP_HOME=$(getent passwd "$user" | cut -d: -f6)
        [[ -z "$ZEP_HOME" ]] && ZEP_HOME="/home/$user"
        sort -u "$ZEP_HOME/.ssh/authorized_keys" -o "$ZEP_HOME/.ssh/authorized_keys"
    done
    sort -u /root/.ssh/authorized_keys -o /root/.ssh/authorized_keys 2>/dev/null || true

    # Populate known_hosts
    echo "Populating SSH known_hosts for zep-users..."
    for i in $(seq 1 "$NUM_NODES"); do
        local user="$(get_node_user "$i")"
        local ZEP_HOME
        ZEP_HOME=$(getent passwd "$user" | cut -d: -f6)
        [[ -z "$ZEP_HOME" ]] && ZEP_HOME="/home/$user"
        cp /root/.ssh/known_hosts "$ZEP_HOME/.ssh/known_hosts" 2>/dev/null || true
        chown "$user:$user" "$ZEP_HOME/.ssh/known_hosts"
        chmod 600 "$ZEP_HOME/.ssh/known_hosts"
    done
}

_setup_sim_hosts() {
    echo "Adding /etc/hosts entries for zep nodes..."
    for i in $(seq 1 "$NUM_NODES"); do
        local fqdn
        fqdn=$(get_node_fqdn "$i")
        if ! grep -q "$fqdn" /etc/hosts 2>/dev/null; then
            echo "127.0.0.1 $fqdn" >> /etc/hosts
        fi
    done
}

_setup_sim_cron() {
    echo "Installing root crontab for snapshot rotation..."
    local CRON_TMP
    CRON_TMP=$(mktemp)
    crontab -l 2>/dev/null | grep -v 'zep.*--rotate' > "$CRON_TMP" || true
    for i in $(seq 1 "$NUM_NODES"); do
        local ds
        ds=$(get_node_ds "$i")
        echo "* * * * * $ZEP_BIN --alias node$i --rotate $ds >/dev/null 2>&1" >> "$CRON_TMP"
    done
    crontab "$CRON_TMP"
    rm -f "$CRON_TMP"
    systemctl restart crond 2>/dev/null || service crond restart 2>/dev/null || true
    echo "  ✅ Cron installed for nodes 1-$NUM_NODES (snapshot rotation every minute)"
}

# ── config generation ─────────────────────────────────────
_generate_config() {
    local CHAIN POLICY
    CHAIN=$(seq 1 "$NUM_NODES" | sed 's/^/node/' | paste -sd, -)
    POLICY=${POLICY:-fail}

    MASTER_CONFIG="/tmp/zep-master.conf"
    echo "Generating $MASTER_CONFIG..."
    cat <<EOF > "$MASTER_CONFIG"
chain=$CHAIN
debug:send_delay=0
policy=$POLICY
role:master:keep:min1=10
role:middle:keep:min1=30
role:sink:keep:min1=90
smtp:from=${SMTP_FROM:-zep@local}
smtp:host=${SMTP_HOST:-127.0.0.1}
smtp:password=${SMTP_PASSWORD:-none}
smtp:port=${SMTP_PORT:-1025}
smtp:protocol=${SMTP_PROTOCOL:-smtp}
smtp:starttls=${SMTP_STARTTLS:-false}
smtp:to=${SMTP_TO:-admin@local}
smtp:user=${SMTP_USER:-none}
alert:critical:threshold=${ALERT_CRITICAL_THRESHOLD:-0}
alert:warn:threshold=${ALERT_WARN_THRESHOLD:-0}
alert:info:threshold=${ALERT_INFO_THRESHOLD:-0}
suspend=false
user=$(get_node_user 1)
zfs:send_opt=-p
zfs:throttle=${ZFS_THROTTLE:-64k}
EOF

    for j in $(seq 1 "$NUM_NODES"); do
        echo "node:node$j:fqdn=$(get_node_fqdn "$j")" >> "$MASTER_CONFIG"
        echo "node:node$j:fs=$(get_node_ds "$j")" >> "$MASTER_CONFIG"
        echo "node:node$j:user=$(get_node_user "$j")" >> "$MASTER_CONFIG"
    done
}

# ── main ──────────────────────────────────────────────────
_main() {
    echo ""
    echo "=== Zeplicator Init ==="
    echo "Mode: $( [[ "$DISTRIBUTED" == "true" ]] && echo distributed || echo simulated )"
    echo "Nodes: $NUM_NODES"

    # Ensure build/zep is ready
    [[ "$DISTRIBUTED" != "true" ]] && make -C "$SCRIPT_DIR/.." > /dev/null 2>&1 || true

    if [[ "$DISTRIBUTED" == "true" ]]; then
        # ── Distributed Setup ─────────────────────────────
        rm -rf /tmp/zep_* 2>/dev/null || true

        # Setup master (node1) locally
        _setup_local_node || exit 1

        # Setup remote nodes (node2..NUM_NODES) via root SSH
        for i in $(seq 2 "$NUM_NODES"); do
            _setup_remote_node "$i" || exit 1
        done

        # SSH mesh: distribute keys across all nodes
        _setup_distributed_mesh

        # /etc/hosts with real IPs
        _setup_distributed_hosts

        # Generate and import config
        _generate_config
        echo "Importing configuration to $(get_node_ds 1)..."
        "$ZEP_BIN" "$(get_node_ds 1)" --alias node1 --config --import "$MASTER_CONFIG"

        # Install cron on all nodes
        _setup_distributed_cron

    else
        # ── Simulated Setup ───────────────────────────────
        _setup_sim_ramdisk

        for i in $(seq 1 "$NUM_NODES"); do
            _setup_sim_pool "$i"
            _setup_sim_user "$i"
        done

        _setup_sim_hosts
        _setup_sim_mesh
        rm -rf /tmp/zep_* 2>/dev/null || true

        _generate_config
        echo "Importing configuration to $(get_node_ds 1)..."
        "$ZEP_BIN" "$(get_node_ds 1)" --alias node1 --config --import "$MASTER_CONFIG"

        _setup_sim_cron
    fi

    echo ""
    echo "=== Initialization complete ==="
    echo "To clean up: bash $SCRIPT_DIR/done.sh"
}

_main
