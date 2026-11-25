# raftis

A Redis-compatible in-memory database written from scratch in plain Java 21, with no frameworks.

It speaks Redis's wire protocol, so the real `redis-cli`, `redis-benchmark` and client libraries such as Jedis work with it unchanged. Writes are saved to disk with an append-only log and snapshots, and crash tests check that **no acknowledged write is lost, even when the process is killed with `kill -9`.**

> **Roadmap:** the next milestone is Raft consensus, which would replicate data across several nodes and fail over automatically without losing acknowledged writes (see [Roadmap](#roadmap)). Today raftis runs as a single node.

## Quickstart

```bash
./gradlew installDist
build/install/raftis/bin/raftis --port 6380 --dir ./data

# in another terminal
redis-cli -p 6380
127.0.0.1:6380> SET greeting hello EX 60
OK
127.0.0.1:6380> ZADD leaderboard 100 alice 250 bob 175 carol
(integer) 3
127.0.0.1:6380> ZRANGE leaderboard 0 -1 WITHSCORES
1) "alice"
2) "100"
3) "carol"
4) "175"
5) "bob"
6) "250"
```

Options: `--port`, `--bind`, `--dir`, `--appendonly yes|no`, `--appendfsync always|everysec|no` (run with `--help` to see them all).

## Features

- **67 commands** across strings, lists, hashes, sorted sets, key expiry, pub/sub and server commands (`INFO`, `CONFIG GET`, `SAVE`, `SHUTDOWN`)
- **RESP protocol**, including pipelining and inline commands (so `telnet` and `nc` work too)
- **Key expiry** both lazily (checked on access) and actively (Redis's random-sampling algorithm, so keys nobody reads still get deleted)
- **Persistence:** an append-only file with `always` / `everysec` / `no` fsync policies, **group commit**, checksummed snapshots and crash-safe file rotation
- **Exact Redis error messages** (`WRONGTYPE ...`, `ERR wrong number of arguments ...`), so client libraries behave as they do with Redis

## Architecture

```
            redis-cli / redis-benchmark / Jedis / any Redis client
                               │  RESP over TCP
                               ▼
  ┌──────────────────── one event-loop thread (Java NIO Selector) ────────────────────┐
  │                                                                                  │
  │  read socket ──► RespParser ──► CommandTable ──► handler ──► Keyspace (in memory)│
  │  (partial reads,  (incremental,   (arity, flags)    │          HashMap + TTL index │
  │   pipelining)      binary-safe)                     │          RList / RHash / ZSet│
  │                                                     ▼                             │
  │                                   write commands ──► AOF buffer                   │
  │                                                                                  │
  │  end of each loop pass:  1. write AOF buffer + fsync   (group commit)            │
  │                          2. only then send replies to clients                    │
  └──────────────────────────────────────────────────────────────────────────────────┘
                               │
                 data/appendonly.<generation>.aof   data/dump.snapshot
```

| Package | What it does |
|---|---|
| `server` | NIO event loop, connections, pub/sub, configuration |
| `protocol` | Incremental RESP parser and reply writer |
| `command` | Command table and the handlers, one file per data type |
| `store` | Keyspace, expiry index, circular-array list, skip-list sorted set |
| `persistence` | Append-only file, snapshots, recovery |

## Design decisions and trade-offs

**One thread, no locks.** Like Redis, a single thread runs every command, so each command is atomic and the data structures need no locks. It's fast because commands are in-memory operations taking microseconds, and the NIO `Selector` means the thread never waits on one slow client. The cost is that a single core does all the work, and one slow command (for example `KEYS *` on a huge dataset) stalls every client.

**Never reply before the write is on disk, but share the fsync.** Replies are only sent at the end of an event-loop pass, *after* that pass's writes have been written to the log (and fsynced in `always` mode). One fsync covers every write in the batch. With fsync on every write, throughput scales from **267 ops/sec with 1 client to 23,700 ops/sec with 200 clients** at almost the same latency ([BENCHMARKS.md](BENCHMARKS.md)).

**Log effects, not commands.** `EXPIRE k 10` is logged as `PEXPIREAT k <absolute ms>`, and `SET k v EX 10` as `SET k v PXAT <absolute ms>`. Otherwise, replaying the log tomorrow would give every key a fresh TTL. Conditional commands such as `SET NX` are logged as their effect (a plain `SET`) or not at all.

**Time stands still during replay.** While the log is being replayed, no key expires. This rebuilds exactly the state that existed, including keys whose TTL passed while the server was down. Those keys are expired normally once loading finishes. (Without this rule, replaying `SET k 5 PXAT <past>` followed by `INCR k` would create `k = 1` with no TTL.)

**Crash-safe snapshots through generations.** AOF files are named `appendonly.<generation>.aof`, and each snapshot records which generation follows it. `SAVE` creates the next AOF, writes the snapshot to a temp file, fsyncs it, then **atomically renames** it into place. That rename is the commit point. A crash on either side of it never loses a write or applies one twice. Redis 7 solved the same problem with a manifest file.

**Why SAVE blocks.** Redis snapshots in the background by calling `fork()` and letting the OS share memory copy-on-write. The JVM can't fork, so `SAVE` pauses the server while it writes. A real fix would need copy-on-write data structures.

**A hand-written skip list.** Sorted sets use a skip list where each pointer also records how many nodes it skips (its "span"), as in Redis. Adding up spans gives `ZRANK` in O(log n). `TreeMap` and `ConcurrentSkipListMap` can't answer rank queries faster than O(n).

**O(1) random sampling for expiry.** Active expiry needs random keys that have a TTL, and a `HashMap` can't produce one in O(1). Keys with a TTL are also kept in an array with an index map, using swap-with-last to remove in O(1).

**Binary-safe throughout.** Keys and values are `byte[]`, never `String`. Since `byte[]` can't be a `HashMap` key (arrays use identity equality), keys are wrapped in `ByteKey`, which compares contents and caches its hash.

## Testing

```bash
./gradlew test                              # 28 tests, about 15 seconds
./gradlew test -Draftis.crashRuns=100       # crash test with 100 kill -9 runs
```

| Test | What it proves |
|---|---|
| `RespParserTest` | Cuts a command at **every byte position** and checks the parser waits for the rest. Also covers pipelining, binary values containing `\r\n`, and malformed input. |
| `ZSetTest` | 50,000 random operations on the skip list checked against a simple reference implementation (ordering, ranks, score ranges). |
| `KeyspaceTest` | TTL edge cases with a fake clock, active expiry, and time standing still during replay. |
| `ServerIntegrationTest` | Every data type through **Jedis**, a real Redis client, including pub/sub. |
| `PersistenceTest` | Restarts with every data type, snapshot plus newer AOF entries, a log cut off mid-command, and rejection of a corrupted snapshot (CRC32). |
| `CrashRecoveryTest` | Runs the server as a separate process, `INCR`s as fast as possible, **SIGKILLs it at a random moment** (sometimes during a `SAVE`), restarts it, and checks no acknowledged increment was lost. **100 out of 100 runs pass.** With the AOF disabled, the same crash loses every write, which shows the test can detect data loss. |

Limitation: `kill -9` simulates a process crash, not a power cut. Data the OS has accepted but not yet flushed survives a process crash, so this test can't tell `always` apart from `no`. Testing real power loss needs fault injection at the disk level.

## Benchmarks

Measured on an Apple M4 Pro (12 cores) against Redis 8.12, with `redis-benchmark` on the same machine, 50 clients and 1M random keys. Full method and raw results: [BENCHMARKS.md](BENCHMARKS.md).

| | Redis 8.12 | raftis |
|---|---|---|
| SET, no pipelining | ~120–136k ops/s | ~142–152k ops/s |
| GET, no pipelining | ~125–141k ops/s | ~128–146k ops/s |
| SET, 16 pipelined | ~0.95–1.0M ops/s | ~1.28–1.57M ops/s |
| GET, 16 pipelined | ~1.13–1.22M ops/s | ~1.41–1.48M ops/s |
| Memory for ~950k small keys | **70 MB** | **555 MB** |

**How to read this honestly:** without pipelining, both are limited by network round trips and perform about the same. With pipelining, raftis is ahead because **it does much less work per command**: Redis also handles ACLs, keyspace notifications, memory accounting, client-side caching, latency tracking and much more. That doesn't make raftis "faster than Redis" in general. **Memory is the real cost of Java:** every key carries several objects with headers, so raftis uses about 8x more memory than Redis for small keys.

## Limitations

- Single node only; replication is the next milestone
- One database (no `SELECT 1`) and no authentication. It binds to `127.0.0.1` by default for that reason.
- `SAVE` blocks while writing (the JVM has no `fork()`)
- The AOF is fully read into memory on startup, so it must be under 2 GB
- No transactions (`MULTI`/`EXEC`), Lua scripting, blocking commands (`BLPOP`), streams or cluster mode

## Roadmap

1. **Raft replication.** Leader election, log replication and snapshots, with the Raft log replacing the AOF, so a write is acknowledged only once a majority of nodes has it.
2. **Linearizable reads** using ReadIndex, plus tunable read consistency (`CONSISTENT` / `LEADER` / `STALE`).
3. **Chaos testing.** Kill, pause and partition nodes in Docker while recording every operation, then check the history with the Porcupine linearizability checker.
4. **Observability.** Prometheus metrics and a live cluster visualizer.
