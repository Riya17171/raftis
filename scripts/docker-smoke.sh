#!/usr/bin/env bash
# Checks a cluster started with `docker compose up --build -d`: replication, failover after
# SIGKILLing the leader's container, and the old leader catching up when it comes back.
set -euo pipefail
cd "$(dirname "$0")/.."

leader_port() {
  for port in 6380 6381 6382; do
    if redis-cli -p "$port" INFO 2>/dev/null | grep -q '^role:leader'; then echo "$port"; return 0; fi
  done
  return 1
}

wait_for_leader() {
  for _ in $(seq 1 150); do
    if port=$(leader_port); then echo "$port"; return 0; fi
    sleep 0.2
  done
  echo "no leader elected within 30 s" >&2
  return 1
}

stale_get() { # read a node's own copy, even on a follower
  printf 'READMODE STALE\nGET %s\n' "$2" | redis-cli -p "$1" 2>/dev/null | tail -n 1
}

leader=$(wait_for_leader)
echo "leader is on port $leader"
curl -sf "http://localhost:$((leader + 2801))/metrics" | grep -q '^raftis_raft_role{role="leader"} 1' \
  || { echo "the leader's /metrics endpoint doesn't report it as leader" >&2; exit 1; }
echo "metrics endpoint OK"
for _ in $(seq 1 100); do redis-cli -p "$leader" INCR counter > /dev/null; done
[ "$(redis-cli -p "$leader" GET counter)" = "100" ]
echo "100 increments acknowledged"

node="node$((leader - 6379))"
docker compose kill "$node"
echo "killed $node (SIGKILL)"
new_leader=$(wait_for_leader)
[ "$new_leader" != "$leader" ]
echo "new leader is on port $new_leader"
value=$(redis-cli -p "$new_leader" GET counter)
[ "$value" = "100" ] || { echo "expected 100 after failover, got '$value'" >&2; exit 1; }
[ "$(redis-cli -p "$new_leader" INCR counter)" = "101" ]
echo "no acknowledged write lost; cluster still accepts writes"

docker compose start "$node"
for _ in $(seq 1 300); do
  [ "$(stale_get "$leader" counter)" = "101" ] && { echo "$node rejoined and caught up"; exit 0; }
  sleep 0.2
done
echo "$node never caught up" >&2
exit 1
