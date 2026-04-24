#!/bin/bash
set -e

# Load configuration if available
SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
CONF_FILE="$SCRIPT_DIR/test.conf"
if [[ -f "$CONF_FILE" ]]; then
    source "$CONF_FILE"
fi

NUM_NODES=${NUM_NODES:-3}
SESSION_NAME="zep-test"

# Check if tmux is installed
if ! command -v tmux &> /dev/null; then
    echo "Error: tmux is not installed. Please install it first."
    exit 1
fi

# Kill the session if it already exists to start fresh
tmux kill-session -t "$SESSION_NAME" 2>/dev/null || true

# Start a new detached session with default dimensions to avoid 'size missing' errors
tmux new-session -d -s "$SESSION_NAME" -x 160 -y 40

# Split horizontally: right pane takes 30% width (leaving 70% for the left pane)
# Using -l (size) with % for newer tmux, or falling back to absolute sizing
tmux split-window -h -l 30% -t "$SESSION_NAME:0.0" 2>/dev/null || tmux split-window -h -l 48 -t "$SESSION_NAME:0.0"

# Now we have:
# Pane 0: Left side (70% width)
# Pane 1: Right side (30% width)

# Split the right pane vertically: lower pane takes 30% height (leaving 70% for the upper pane)
tmux split-window -v -l 30% -t "$SESSION_NAME:0.1" 2>/dev/null || tmux split-window -v -l 12 -t "$SESSION_NAME:0.1"

# Split the left pane vertically: lower pane takes 30% height (for SMTP Server)
tmux split-window -v -l 30% -t "$SESSION_NAME:0.0" 2>/dev/null || tmux split-window -v -l 12 -t "$SESSION_NAME:0.0"

# Now we have:
# Pane 0: Left main test window (Top)
# Pane 1: Left lower (SMTP Server)
# Pane 2: Right upper (watch status)
# Pane 3: Right lower (bash for traffic/network simulation)

# Set up left main pane (Pane 0)
tmux send-keys -t "$SESSION_NAME:0.0" "clear" C-m
tmux send-keys -t "$SESSION_NAME:0.0" "echo '=== Main Test Window ==='" C-m
tmux send-keys -t "$SESSION_NAME:0.0" "echo 'Run your test scripts or interact with zep here.'" C-m

# Set up left lower pane (Pane 1) - SMTP Server
tmux send-keys -t "$SESSION_NAME:0.1" "clear" C-m
tmux send-keys -t "$SESSION_NAME:0.1" "$SCRIPT_DIR/smtp_debug.py 1025 --show-mail-only --use-color" C-m

# Set up right upper pane (Pane 2) - Watch zep status
# We'll default to monitoring the dataset on the first node
DATASET_TO_WATCH="zep-node-1/test-1"
ZEP_BIN="$SCRIPT_DIR/../build/zep"
tmux send-keys -t "$SESSION_NAME:0.2" "watch --color -n 10 $ZEP_BIN $DATASET_TO_WATCH --status --force-color" C-m

# Set up right lower pane (Pane 3) - Traffic / Network simulator
tmux send-keys -t "$SESSION_NAME:0.3" "clear" C-m
tmux send-keys -t "$SESSION_NAME:0.3" "echo '=== Simulator Shell ==='" C-m
tmux send-keys -t "$SESSION_NAME:0.3" "echo 'Use this pane to edit /etc/hosts or generate disk traffic.'" C-m

# Select the main left pane as the active one
tmux select-pane -t "$SESSION_NAME:0.0"

# Attach to the session
echo "Attaching to tmux session '$SESSION_NAME'..."
tmux set -t "$SESSION_NAME" mouse on
tmux attach-session -t "$SESSION_NAME"
