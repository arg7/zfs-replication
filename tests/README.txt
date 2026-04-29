╔══════════════════════════════════════════════════════════════════╗
║            Zeplicator Test Console (tzepcon)                     ║
╚══════════════════════════════════════════════════════════════════╝

Welcome! This tmux session runs the Zeplicator replication test
suite in a live, observable environment.

  PANE LAYOUT
  ──────────
  pane 0 (main, left top)   — test suite output
  pane 1 (left bottom)      — SMTP debug server (alerts)
  pane 2 (right top)        — zep --status watcher
  pane 3 (right bottom)     — simulator shell (you are here)

  TEST CONTROLS (pane 3)
  ──────────────────────
  start                      run all 14 tests
  start --test 13 14         run only resilience tests
  start --test 2 12          run only tests 2 and 12
  start --skip 11 13         skip resume and resilience tests
  stop                       abort running test suite
  q                          stop tests and exit tmux session

  CONFIG (pane 3)
  ───────────────
  config get                 list all zep: properties
  config get chain           read zep:chain
  config get policy          read zep:policy
  config set policy=resilience  switch to resilience mode
  config set zfs:send_opt=-L    enable large blocks
  config rm  policy          reset to default
  config set <prop>=<val>    set any zep: property

  KEY CONFIG PROPERTIES (on zep-node-1/test-1)
  ─────────────────────────────────────────────
  zep:chain           node1,node3,node2 (replication order)
  zep:policy          fail | resilience (default: fail)
  zep:zfs:send_opt    extra flags for zfs send (eg. -Lp)
  zep:zfs:recv_opt    extra flags for zfs recv (default: -F)
  zep:debug:throttle  zfs pipe rate limit (eg. 32k, 128k)
  zep:debug:send_timeout  inject disconnect into zfs pipe (seconds)
  zep:suspend         true | false (pause replication)

  SIMULATOR CHEATSHEET (pane 3)
  ──────────────────────────────
  # Isolate a node (makes it unreachable)
  sed -i '/zep-node-2.local/d' /etc/hosts

  # Restore a node
  echo '127.0.0.1 zep-node-2.local' >> /etc/hosts

  # Generate disk traffic on node1
  dd if=/dev/urandom of=/zep-node-1/test-1/junk.bin bs=1M count=10

  # Watch a specific node's snapshots
  watch -n 5 'zfs list -t snap -r zep-node-2/test-2'

  # Send keystrokes to pane 2 (status watcher)
  keystroke 'watch -n 5 zfs list -t snap -r zep-node-1/test-1'

  TEST OVERVIEW
  ─────────────
   1  INIT_CLEAN       — initial replication, clean dest
   2  INCREMENTAL       — normal incremental run
   3  FOREIGN_DATASET   — node3 has alien snapshots
   4  MISSING_PERMS     — revoked ZFS permissions
   5  DIVERGENCE        — split-brain divergence detected
   6  DIVERGENCE_OVERRIDE — -y forces through divergence
   7  NON_MASTER_SKIP   — non-master skips snapshot creation
   8  MISSING_POOL      — target pool exported
   9  STATUS            — status command works
  10  ROTATE            — retention keeps count
  11  RESUME            — interrupted transfer resumes
  12  RESUME_FAILED     — mid-transfer snapshot loss
  13  RESILIENCE_NODE2_OFFLINE — offline node, partial success
  14  RESILIENCE_NODE2_RECOVERY — restored node, full success
  15  PROMOTE_NODE3       — promote node3 to master
  16  PROMOTE_NODE1_BACK   — restore node1 as master

  TO QUIT
  ───────
  q                             stop tests + kill session
  Ctrl-B d                      detach (leave session running)
