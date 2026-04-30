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
  list                       list available tests
  log                        cat last test log
  log 05                     cat test 05 log
  start                      run all 16 tests
  start --test 7 8           run only resilience tests
  start --test 2 12          run only tests 2 and 12
  start --skip 5 6           skip resume tests
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

  TEST OVERVIEW
  ─────────────
  Type 'list' to see all tests.
  Type 'log' to see latest test log.

  TO QUIT
  ───────
  q                             stop tests + kill session
  Ctrl-B d                      detach (leave session running)
