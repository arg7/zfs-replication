#!/bin/bash
# sim.sh — simulator pane convenience wrappers
# Sourced automatically in tzepcon pane 3

SDIR="$(dirname "$(readlink -f "${BASH_SOURCE[0]:-$0}")")"
export PS1=$'\e[36msim>\e[0m '
SESSION="${ZEP_SESSION:-zep-test}"
DS="zep-node-1/test-1"

config() {
    local cmd="${1:-}"; shift 2>/dev/null || true
    case "$cmd" in
        get)
            if [[ -z "${1:-}" ]]; then
                zep "$DS" --alias node1 --config
            else
                zfs get -H -o value "zep:${1}" "$DS" 2>/dev/null
            fi
            ;;
        set)
            [[ -z "${1:-}" ]] && { echo "Usage: config set <prop>=<val>"; return 1; }
            zep "$DS" --alias node1 --config --all "$1"
            ;;
        rm)
            [[ -z "${1:-}" ]] && { echo "Usage: config rm <prop>"; return 1; }
            zep "$DS" --alias node1 --config --all --clear "$1"
            ;;
        *)
            echo "Usage: config {get|set|rm} [args]"
            echo "  config get            list all properties"
            echo "  config get <prop>     read one property"
            echo "  config set <prop>=<val>  assign a property"
            echo "  config rm  <prop>     remove a property"
            ;;
    esac
}

list() {
    "$SDIR/zep_replication_tests.sh" --list
}

log() {
    local id="${1:-}"
    if [[ -z "$id" ]]; then
        id=$(ls -1t /tmp/test[0-9][0-9]*.log 2>/dev/null | head -1 | sed 's|.*/test||;s|\.log$||')
        [[ -z "$id" ]] && { echo "No logs found."; return 1; }
    fi
    # Pad bare prefix to 2 digits: "6" -> "06", "6-3" -> "06-3"
    id=$(echo "$id" | sed -E 's/^([0-9])([^0-9]|$)/0\1\2/')
    less "/tmp/test${id}.log" 2>/dev/null || echo "Log not found: /tmp/test${id}.log"
}

stop() {
    tmux send-keys -t "${SESSION}:0.0" C-c
    sleep 0.4
    local pane_pid child
    pane_pid=$(tmux list-panes -t "${SESSION}:0.0" -F '#{pane_pid}' 2>/dev/null)
    if [[ -n "$pane_pid" ]]; then
        child=$(pgrep -P "$pane_pid" -f "zep_replication_tests" | head -1)
        [[ -n "$child" ]] && kill "$child" 2>/dev/null && echo "  Killed PID $child"
    fi
    echo "  Test run stopped."
}

start() {
    tmux send-keys -t "${SESSION}:0.0" C-c
    sleep 0.4
    tmux send-keys -t "${SESSION}:0.0" "clear" C-m
    sleep 0.1
    tmux send-keys -t "${SESSION}:0.0" "${SDIR}/zep_replication_tests.sh $*" C-m
    echo "  Started: zep_replication_tests.sh $*"
}

keystroke() {
    local pane="${ZEP_PANE:-0.2}"
    tmux send-keys -t "${SESSION}:${pane}" "$*" C-m
    echo "  Sent to pane ${pane}: $*"
}

q() {
    stop 2>/dev/null
    tmux kill-session -t "$SESSION" 2>/dev/null
    echo "  Session $SESSION killed."
}
