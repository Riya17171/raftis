# Benchmarks

## Setup

- **Machine:** Apple M4 Pro, 12 cores, 24 GB RAM, macOS
- **Java:** OpenJDK 24.0.2, default flags (G1 garbage collector)
- **Redis:** 8.12.0 from Homebrew, `io-threads 1` (the default)
- **Client:** `redis-benchmark` 8.12.0 on the same machine, so the client competes with the server for CPU. Treat these as comparisons between the two servers, not absolute numbers.
- Both servers ran with persistence off (`--appendonly no`, `--save ""`) unless a table says otherwise.
- raftis got a warm-up run first so the JIT compiler could optimize the hot code. Each configuration ran 3 times.

## Throughput: 50 clients, 1M random keys

```bash
redis-benchmark -p <port> -t set,get -n 300000 -c 50 -r 1000000 -P <1|16> --csv
```

Requests per second (p50 latency in ms):

| | Run | Redis SET | raftis SET | Redis GET | raftis GET |
|---|---|---|---|---|---|
| No pipelining | 1 | 135,685 (0.49) | 141,911 (0.42) | 133,869 (0.50) | 146,341 (0.41) |
| | 2 | 120,096 (0.55) | 151,515 (0.41) | 140,647 (0.46) | 127,660 (0.43) |
| | 3 | 122,299 (0.54) | 142,586 (0.42) | 124,896 (0.53) | 130,095 (0.43) |
| 16 pipelined | 1 | 986,842 (0.94) | 1,282,051 (0.75) | 1,132,076 (0.88) | 1,408,451 (0.72) |
| | 2 | 946,372 (1.00) | 1,363,636 (0.68) | 1,162,791 (0.84) | 1,477,833 (0.65) |
| | 3 | 996,678 (0.86) | 1,570,681 (0.55) | 1,219,512 (0.75) | 1,442,308 (0.66) |

Turning off Redis's `latency-tracking` made no meaningful difference (~0.96–0.98M SET, ~1.22–1.23M GET pipelined).

**Why raftis can come out ahead:** it does far less per command. Redis also runs ACL checks, keyspace notifications, memory accounting, client-side caching, latency tracking and incremental rehashing. Without pipelining, both servers are limited by network round trips.

**Sanity check:** after `SET` with 100 random keys, raftis reported `DBSIZE 100` and stored the right values, so the benchmark measured real work and not fast error replies.

## Memory: ~950k keys with 3-byte values

```bash
redis-benchmark -p <port> -t set -n 3000000 -c 50 -r 1000000 -P 32
```

| | Keys | Process RSS | Notes |
|---|---|---|---|
| Redis 8.12 | 950,241 | **70 MB** | compact C structures |
| raftis | 949,994 | **555 MB** | 298 MB of live heap; the rest is heap the JVM has reserved |

Each raftis key costs several Java objects (`ByteKey`, its `byte[]`, the value's `byte[]`, a `HashMap` node), and every object has a header of 12–16 bytes. Possible improvements: store keys and values off-heap in large `ByteBuffer`s, or use an open-addressing hash table of primitives.

## Durability cost: SET, 50 clients, no pipelining

Both servers had their append-only file on, with the same fsync policy.

| `appendfsync` | Redis ops/s (p50 ms) | raftis ops/s (p50 ms) |
|---|---|---|
| `no` | 121,877 (0.54) | 152,207 (0.40) |
| `everysec` | 127,959 (0.50) | 149,254 (0.40) |
| `always` | 6,683 (9.8) | 7,247 (9.0) |

In `always` mode, both servers are limited by how long the disk takes to fsync.

## Group commit: `appendfsync always`, varying the number of clients

```bash
redis-benchmark -p 6380 -t set -c <clients> -r 100000 --csv
```

| Clients | raftis ops/s | p50 latency (ms) |
|---|---|---|
| 1 | 267 | 4.1 |
| 10 | 1,503 | 8.0 |
| 50 | 7,680 | 8.0 |
| 200 | 23,697 | 9.9 |

With one client, every write waits for its own fsync. With more clients, writes arriving during the same event-loop pass share one fsync. Throughput grows 89x while latency barely changes, and no write is acknowledged before it is on disk.
