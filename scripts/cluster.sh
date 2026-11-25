#!/usr/bin/env bash
# Runs a local 3-node raftis cluster.
#   client ports: 6380, 6381, 6382     Raft ports: 7380, 7381, 7382
#   metrics: http://localhost:9181/metrics, 9182, 9183
#
# usage: scripts/cluster.sh start | stop | status | kill <id> | restart <id>
#   kill <id>     SIGKILLs a node, like a crash or power loss
#   restart <id>  starts a stopped or killed node again; it catches up from the others
set -euo pipefail
cd "$(dirname "$0")/.."

BIN=build/install/raftis/bin/raftis
MEMBERS="1=127.0.0.1:6380:7380,2=127.0.0.1:6381:7381,3=127.0.0.1:6382:7382"
DATA=${RAFTIS_CLUSTER_DIR:-cluster-data}

start_node() {
  local id=$1
  mkdir -p "$DATA/node$id"
  "$BIN" --node-id "$id" --cluster "$MEMBERS" --dir "$DATA/node$id" --metrics-port $((9180 + id)) >> "$DATA/node$id.log" 2>&1 &
  echo $! > "$DATA/node$id.pid"
  echo "started node $id (client port $((6379 + id)), pid $!, log $DATA/node$id.log)"
}

case "${1:-}" in
  start)
    [ -x "$BIN" ] || ./gradlew installDist -q
    for id in 1 2 3; do start_node "$id"; done
    ;;
  stop)
    for id in 1 2 3; do redis-cli -p $((6379 + id)) SHUTDOWN > /dev/null 2>&1 || true; done
    echo "stopped"
    ;;
  status)
    for id in 1 2 3; do
      port=$((6379 + id))
      info=$(redis-cli -p "$port" INFO 2>/dev/null | tr -d '\r' | grep -E '^(role|term|commit_index|last_applied):' | tr '\n' ' ' || true)
      echo "node $id (:$port)  ${info:-DOWN}"
    done
    ;;
  kill)
    kill -9 "$(cat "$DATA/node${2:?node id}.pid")" && echo "killed node $2 (SIGKILL)"
    ;;
  restart)
    start_node "${2:?node id}"
    ;;
  *)
    echo "usage: $0 start | stop | status | kill <id> | restart <id>"
    exit 1
    ;;
esac
