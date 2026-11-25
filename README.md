# raftis

A Redis-compatible database written from scratch in plain Java 21 (no frameworks). It **replicates data across nodes with the Raft consensus algorithm**, so a write is acknowledged only once a majority of nodes has it on disk.

Plain Redis replicates asynchronously, so a failover can lose writes the client was already told succeeded. Here, killing the leader with `kill -9` loses nothing: the cluster elects a new leader in about 200 ms and keeps serving. Real `redis-cli`, `redis-benchmark` and client libraries such as Jedis work with it unchanged.

## Quickstart

**A 3-node cluster on one machine:**

```bash
./gradlew installDist
scripts/cluster.sh start          # nodes on client ports 6380, 6381, 6382
scripts/cluster.sh status         # shows which node is the leader

redis-cli -p 6380 SET greeting hello       # on a follower you'll get: NOTLEADER 127.0.0.1:<leader port>
scripts/cluster.sh kill 2                  # SIGKILL a node (try the leader)
scripts/cluster.sh status                  # a new leader is elected in ~200 ms; no data lost
scripts/cluster.sh restart 2               # it rejoins and catches up automatically
scripts/cluster.sh stop
```

**A single standalone server** (no replication; append-only file for durability):

```bash
build/install/raftis/bin/raftis --port 6380 --dir ./data
```

Run `build/install/raftis/bin/raftis --help` to see all options.

## Features

- **68 commands** across strings, lists, hashes, sorted sets, key expiry, pub/sub and server commands. Exact Redis error messages and the RESP protocol (including pipelining), so clients behave as they do with Redis.
- **Raft replication:**
  - leader election and log replication
  - batching and pipelining
  - log compaction with snapshots, and `InstallSnapshot` for nodes that fall far behind
  - automatic failover
- **Linearizable reads** with ReadIndex, no log write needed. `READMODE` lets each connection choose:

  | Mode | Behavior |
  |---|---|
  | `CONSISTENT` (default) | linearizable reads via ReadIndex |
  | `LEADER` | leader reads without confirming it's still the leader |
  | `STALE` | any node answers, so reads scale but may be behind |
- **Durability.** In cluster mode, the Raft log with group commit and crash-safe snapshots. In standalone mode, an append-only file (`always` / `everysec` / `no`) with crash-safe snapshot rotation.
- **Deterministic expiry across nodes.** Keys expire identically on every node, whatever each machine's clock says.

## Architecture

```
      redis-cli / Jedis / any Redis client
                   │ RESP over TCP
                   ▼
 ┌─────────────── node (one event-loop thread, Java NIO) ────────────────┐
 │                                                                       │
 │  client socket ─► RespParser ─► command ─┬─ write ─► RaftNode.propose │
 │                                          ├─ read ──► ReadIndex check  │
 │                                          └─ local (PING, INFO...)     │
 │                                                                       │
 │  RaftNode (pure state machine: no threads, no I/O, no clock)          │
 │     ▲ tick() every 10 ms        │ messages                             │
 │     │ step(message)             ▼                                      │
 │  PeerTransport (TCP) ◄──► other nodes                                 │
 │                                                                       │
 │  each loop pass:  fsync Raft log once (group commit)                  │
 │                   → then send Raft messages → then reply to clients   │
 │                                                                       │
 │  committed entries ─► RaftKv.apply ─► Keyspace (in memory)            │
 └───────────────────────────────────────────────────────────────────────┘
          data/raft-log   data/raft-state   data/raft-snapshot
```

| Package | What it does |
|---|---|
| `raft` | Raft core (`RaftNode`, `RaftLog`), disk storage, TCP transport, message encoding |
| `server` | NIO event loop, connections, `RaftKv` (connects Raft to the commands), pub/sub |
| `protocol` | Incremental RESP parser and reply writer |
| `command` | Command table and handlers, one file per data type |
| `store` | Keyspace, expiry index, circular-array list, skip-list sorted set |
| `persistence` | Standalone mode: append-only file and snapshots |

## Design decisions

### Raft

**Raft as a pure state machine.** `RaftNode` has no threads, sockets or clock. The event loop feeds it ticks and messages and sends what it outputs, the way etcd's Raft library works. That keeps all Raft state on one thread with no locks, and it makes the code fully deterministic, which is what the simulation tests depend on.

**Persist before you speak.** Raft requires the term, vote and log to be on disk before a node sends any message that depends on them. Every outgoing message is held until the loop pass's single `fsync`, so this holds by construction, and one fsync covers every entry in the pass (group commit).

**The no-op and Figure 8.** A new leader appends a no-op entry and only counts replicas for entries from its own term. Figure 8 of the Raft paper shows that an older-term entry stored on a majority can still be overwritten, so counting it as committed would lose data. The no-op also lets ReadIndex start working right away.

**ReadIndex instead of logging reads.** The leader records its commit index, confirms it's still the leader through one round of heartbeats acknowledged by a majority, waits until it has applied up to that index, and then reads from memory. Reads arriving close together share one confirmation round. A deposed leader cut off in a minority can never serve a stale `CONSISTENT` read.

### Making every node identical

**Time comes from the log.** The leader stamps every entry with its clock reading, and every node applies the entry as if it were that moment. So `SET k v EX 10`, `INCR` on a key that's about to expire and `SET NX` make identical decisions on every node. Without this, each machine's clock would decide differently and the replicas would drift apart.

**Expiry goes through the log.** Reads never delete expired keys; they just report them as missing. The leader finds expired keys and proposes an internal `RAFTIS.EXPIRE k...`, which each node applies by re-checking those keys at the entry's timestamp. A plain `DEL` would be wrong: it could delete a newer value written after the leader noticed the expiry.

### Talking to clients

**Replies stay in order, and writes still pipeline.** A client's consecutive writes all go into the log at once; Raft applies them in log order, so their replies come back in order. A read or other command after a write waits until the writes before it have been answered.

**Honest failure replies.** There are two kinds of failed write:
- `ERR ... not applied ... safe to retry` means a different entry took that log slot, so the command can never apply.
- `UNCERTAIN ...` means the leader lost leadership before confirming, so the write may or may not have applied.

### Durability on disk

**Crash-safe storage.** Log records carry CRC32 checksums, and a record torn by a crash is cut off at startup. It was never acknowledged, because nothing is sent before fsync. The term, vote and snapshots are written to a temp file, fsynced, then atomically renamed into place.

### Standalone mode

This is single-node mode, without Raft.
- **One thread, no locks,** like Redis. Replies are sent only after the append-only file is written (group commit).
- **Effects are logged, not commands:** relative TTLs are stored as absolute times.
- **Snapshots rotate the append-only file safely,** using generation-numbered files.
- **Sorted sets use a hand-written skip list** that tracks how many nodes each pointer skips, so `ZRANK` is O(log n). `TreeMap` can't find a rank faster than O(n).

## Testing

```bash
./gradlew test                                                  # 44 tests, about 25 s
./gradlew test --tests '*RaftSimulationTest*' -Draftis.simSeeds=3000
./gradlew test --tests '*ClusterCrashTest*' -Draftis.clusterCrashRuns=20
```

**Deterministic simulation.** `RaftSimulationTest` runs whole 3- and 5-node clusters on simulated time. The simulated network drops, duplicates, delays and reorders messages, and nodes are crashed, quick-rebooted, partitioned and cut off one way, all driven by one seed, so any failure replays exactly. It checks the following continuously:

| Property | Meaning |
|---|---|
| Election Safety | at most one leader per term |
| State Machine Safety | no two nodes ever apply different entries at the same index |
| Linearizable reads | a read sees every write acknowledged before it started |
| Durability | every acknowledged write survives |
| Liveness | the cluster recovers once failures stop |

> **3000 simulated clusters, with 4.09 million acknowledged writes, 1.12 million linearizable reads and 59,009 injected faults. No violations.**

**Testing the tests.** I planted 6 classic Raft bugs one at a time to check that the tests catch them:

| Planted bug | Caught by |
|---|---|
| Voting ignores how up to date the candidate's log is | simulation: State Machine Safety violation |
| ReadIndex skips the majority confirmation | simulation: stale read detected |
| Committing without a majority | simulation: State Machine Safety violation |
| A delayed message truncates entries the node already has | simulation: "would delete committed entry" check |
| **Committing an old-term entry by counting replicas (Figure 8)** | **`RaftScenarioTest`**: Figure 8 rebuilt step by step |
| **The vote isn't saved to disk before replying** | **`RaftScenarioTest`**: a node votes, reboots, and is asked again |

The random simulation missed the last two in thousands of runs, because each needs an exact sequence of events. That's why they have hand-written scenario tests.

**Real processes and real disks:**

| Test | What it proves |
|---|---|
| `ClusterCrashTest` | 3 server processes. A client `INCR`s as fast as it can while the leader is **SIGKILLed at random moments** (20 rounds passed). Every acknowledged increment survives. |
| `ClusterIntegrationTest` | A 3-node cluster through Jedis: redirects, stale reads, losing the leader, expiry replicated through the log, a lagging node catching up through `InstallSnapshot`, and restarting the whole cluster. |
| `FileRaftStorageTest` | Torn-record repair, truncation, snapshots, CRC rejection, message encoding. |
| `CrashRecoveryTest` | Standalone mode: **100 out of 100** `kill -9` runs with no acknowledged write lost. |
| Parser, skip list, keyspace and Jedis tests | Cover the single-node basics. |

## Benchmarks

Measured on an Apple M4 Pro with all 3 nodes on **one laptop, sharing one SSD**, using `redis-benchmark` on the same machine. Full results: [BENCHMARKS.md](BENCHMARKS.md).

| 3-node cluster | ops/sec | p50 latency |
|---|---|---|
| `SET`, 1 client | ~100 | 12–13 ms |
| `SET`, 50 clients | ~1,140–1,260 | 52–61 ms |
| `SET`, 200 clients | ~4,670–4,750 | 54–57 ms |
| `SET`, 50 clients, 16 pipelined | ~17,500–17,900 | 59–60 ms |
| `GET`, 50 clients, linearizable (ReadIndex) | **~96,000–98,000** | **0.5–0.6 ms** |
| Failover after `kill -9` of the leader | **171–264 ms** (5 runs) | |

**Why writes are slow here, and why that's the disk, not Raft.** Every write needs an fsync on the leader and on a follower, about 4 ms each on this Mac, and the three processes take turns on the same SSD. With fsync turned off (`--appendfsync no`, for measurement only, because it gives up durability on power loss), the same cluster does **~87,000–105,000 writes/sec, or ~530,000 pipelined**, at 0.5 ms p50. On real servers, each node has its own disk.

Standalone mode vs Redis 8.12: about equal without pipelining (both are limited by network round trips), and faster with pipelining because raftis does far less work per command. raftis also uses **about 8x more memory** because of Java object overhead. Details are in BENCHMARKS.md.

## Limitations

- **No membership changes.** The node list is fixed at startup; adding or removing nodes needs joint consensus.
- **No PreVote or CheckQuorum.** A node that rejoins after a partition can force an unnecessary election.
- **Retries can apply twice.** A client that retries after an `UNCERTAIN` reply can apply an `INCR` twice. The fix is client sessions with sequence numbers (Raft thesis §6.3).
- **Snapshots block.** They are taken on the event loop and sent as one message, not in chunks.
- **Single machine only so far.** The cluster has been tested on one machine, not across a real network with real partitions. That's Phase 4.
- **Narrower feature set than Redis.** Pub/sub stays on one node, there's no auth and only one database. There's no `MULTI`/`EXEC`, Lua scripting or cluster sharding.

## Roadmap

1. **Chaos testing:** Docker, network partitions, and history checking with the Porcupine linearizability checker
2. **Observability:** Prometheus metrics and a live cluster visualizer (kill a node and watch the cluster recover)
