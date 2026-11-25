# Benchmarks

1. [Standalone mode vs Redis](#standalone-mode-vs-redis)
2. [Raft cluster (3 nodes)](#raft-cluster-3-nodes)

## Standalone mode vs Redis

### Setup

- **Machine:** Apple M4 Pro, 12 cores, 24 GB RAM, macOS
- **Java:** OpenJDK 24.0.2, default flags (G1 garbage collector)
- **Redis:** 8.12.0 from Homebrew, `io-threads 1` (the default)
- **Client:** `redis-benchmark` 8.12.0 on the same machine, so the client competes with the server for CPU. Treat these as comparisons between the two servers, not absolute numbers.
- Both servers ran with persistence off (`--appendonly no`, `--save ""`) unless a table says otherwise.
- raftis got a warm-up run first so the JIT compiler could optimize the hot code. Each configuration ran 3 times.

### Throughput: 50 clients, 1M random keys

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

### Memory: ~950k keys with 3-byte values

```bash
redis-benchmark -p <port> -t set -n 3000000 -c 50 -r 1000000 -P 32
```

| | Keys | Process RSS | Notes |
|---|---|---|---|
| Redis 8.12 | 950,241 | **70 MB** | compact C structures |
| raftis | 949,994 | **555 MB** | 298 MB of live heap; the rest is heap the JVM has reserved |

Each raftis key costs several Java objects (`ByteKey`, its `byte[]`, the value's `byte[]`, a `HashMap` node), and every object has a header of 12–16 bytes. Possible improvements: store keys and values off-heap in large `ByteBuffer`s, or use an open-addressing hash table of primitives.

### Durability cost: SET, 50 clients, no pipelining

Both servers had their append-only file on, with the same fsync policy.

| `appendfsync` | Redis ops/s (p50 ms) | raftis ops/s (p50 ms) |
|---|---|---|
| `no` | 121,877 (0.54) | 152,207 (0.40) |
| `everysec` | 127,959 (0.50) | 149,254 (0.40) |
| `always` | 6,683 (9.8) | 7,247 (9.0) |

In `always` mode, both servers are limited by how long the disk takes to fsync.

### Group commit: `appendfsync always`, varying the number of clients

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

---

## Raft cluster (3 nodes)

### Setup

- All 3 nodes on the **same laptop** (Apple M4 Pro, macOS), **sharing one SSD**, with `redis-benchmark` on the same machine
- 100,000 random keys, 3-byte values; each configuration ran twice
- Default settings: 150–300 ms election timeout, 50 ms heartbeats, a snapshot every 100,000 entries

```bash
redis-benchmark -p <leader port> -t set -n <N> -c <clients> -P <pipeline> -r 100000 --csv
```

### Writes with fsync on (safe, the default)

| Workload | Run 1: ops/s (p50 / p99 ms) | Run 2: ops/s (p50 / p99 ms) |
|---|---|---|
| SET, 1 client | 100 (13.0 / 18.9) | 101 (12.1 / 20.9) |
| SET, 50 clients | 1,136 (61.5 / 75.6) | 1,265 (52.0 / 66.6) |
| SET, 200 clients | 4,747 (57.2 / 66.0) | 4,673 (54.1 / 75.3) |
| SET, 50 clients, 16 pipelined | 17,490 (60.3 / 109.6) | 17,913 (58.6 / 111.6) |

### Writes with fsync off (`--appendfsync no`, for measurement only)

| Workload | Run 1: ops/s (p50 / p99 ms) | Run 2: ops/s (p50 / p99 ms) |
|---|---|---|
| SET, 1 client | 13,158 (0.09 / 1.8) | 13,947 (0.08 / 2.5) |
| SET, 50 clients | 87,260 (0.53 / 84.7) | 89,047 (0.52 / 36.5) |
| SET, 200 clients | 104,603 (1.8 / 65.1) | 105,430 (1.9 / 34.9) |
| SET, 50 clients, 16 pipelined | 533,808 (1.3 / 58.0) | 525,394 (1.3 / 38.7) |

The p99 spikes are mostly snapshot pauses: at these rates a 100,000-entry snapshot happens several times a second, and it runs on the event loop.

### What the two tables show

Each committed write needs an fsync on the leader and on at least one follower, about 4 ms each here. The three processes also queue behind each other on the same drive. Throughput grows with the number of clients because more writes share each fsync (group commit), while latency stays flat at the cost of a few queued fsyncs.

Turning fsync off shows the Raft code itself can handle about 100,000 replicated writes/sec, or 500,000 pipelined. **The disk is the bottleneck, not the consensus code.** On real servers each node has its own disk, often NVMe with fsync well under 1 ms.

**How it was diagnosed:** writes stayed at ~60 ms whether there were 10 or 200 clients, while throughput grew in proportion to the client count. That pattern means a fixed per-round cost, not overload. Tracing each write from proposal to commit and to the client's next request showed the time was spent waiting on fsync, not in Raft or the network.

### Linearizable reads (ReadIndex)

| Workload | Run 1: ops/s (p50 / p99 ms) | Run 2: ops/s (p50 / p99 ms) |
|---|---|---|
| GET, 50 clients, `READMODE CONSISTENT` | 98,425 (0.54 / 2.5) | 95,785 (0.59 / 3.1) |

Reads never touch the disk. The leader confirms it's still the leader with one heartbeat round, which all reads arriving close together share, and then reads from memory.

### Failover

`kill -9` the leader, then measure until another node reports `role:leader`:

| Kill | Old leader → new leader | Time |
|---|---|---|
| 1 | node 1 → node 3 | 210 ms |
| 2 | node 3 → node 2 | 235 ms |
| 3 | node 2 → node 3 | 171 ms |
| 4 | node 3 → node 2 | 200 ms |
| 5 | node 2 → node 1 | 264 ms |

That matches the randomized 150–300 ms election timeout: followers notice the missing heartbeats, and the first to time out wins.
