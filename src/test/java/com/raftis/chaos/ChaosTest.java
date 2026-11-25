package com.raftis.chaos;

import com.raftis.chaos.LinearizabilityChecker.Result;
import com.raftis.chaos.LinearizabilityChecker.Verdict;
import com.raftis.chaos.Op.Outcome;
import com.raftis.chaos.Op.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.exceptions.JedisException;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Jepsen-style chaos test: 5 real server processes, 10 concurrent clients reading, writing and
 * incrementing 5 shared keys, and a nemesis injecting partitions, crashes and freezes. Every
 * operation is recorded with its start and end time, and at the end the whole history is
 * checked for linearizability.
 *
 * <pre>
 * ./gradlew test --tests '*ChaosTest*' -Draftis.chaosSeconds=300      # a longer run
 * ./gradlew test --tests '*ChaosTest*' -Draftis.chaosReadMode=LEADER  # watch it catch stale reads
 * </pre>
 *
 * The full report and history are written to {@code build/chaos/}.
 */
class ChaosTest {
    private static final int NODES = 5;
    private static final int CLIENTS = 10;
    private static final String[] KEYS = {"k0", "k1", "k2", "k3", "k4"};
    private static final int CLIENT_TIMEOUT_MS = 1500;
    private static final Path REPORT_DIR = Path.of("build", "chaos");

    @TempDir
    Path dir;

    @Test
    void staysLinearizableThroughPartitionsCrashesAndPauses() throws Exception {
        int seconds = Integer.getInteger("raftis.chaosSeconds", 30);
        long seed = Long.getLong("raftis.chaosSeed", new Random().nextLong());
        String readMode = System.getProperty("raftis.chaosReadMode", "CONSISTENT");

        // Snapshot every 200 entries so log compaction and InstallSnapshot happen in the middle of faults.
        try (ChaosCluster cluster = new ChaosCluster(NODES, dir, List.of("--snapshot-entries", "200"))) {
            cluster.startAll();
            assertTrue(cluster.awaitLeader(20_000) > 0, "the cluster never elected a first leader");

            long t0 = System.nanoTime();
            LongSupplier clock = () -> System.nanoTime() - t0;
            Queue<Op> history = new ConcurrentLinkedQueue<>();
            Queue<String> unexpected = new ConcurrentLinkedQueue<>();
            Random rnd = new Random(seed);
            Nemesis nemesis = new Nemesis(cluster, new Random(rnd.nextLong()), clock);

            List<Client> clients = new ArrayList<>();
            List<Thread> threads = new ArrayList<>();
            for (int c = 1; c <= CLIENTS; c++) {
                Client client = new Client(c, cluster, new Random(rnd.nextLong()), clock, history, unexpected, readMode);
                clients.add(client);
                threads.add(Thread.ofPlatform().name("client-" + c).start(client));
            }
            Thread nemesisThread = Thread.ofPlatform().name("nemesis").start(nemesis);

            Thread.sleep(seconds * 1000L);
            nemesis.stop();
            nemesisThread.join();
            cluster.healEverything();
            nemesis.note("final heal: faults stop; clients keep going for 5 s");
            long healedAt = clock.getAsLong();
            Thread.sleep(5000);
            clients.forEach(Client::stop);
            for (Thread t : threads) t.join();

            // Liveness: once the faults stop, the cluster must serve writes again.
            long writesAfterHeal = history.stream()
                    .filter(op -> op.type() != Type.READ && op.outcome() == Outcome.OK && op.invokedAt() > healedAt).count();

            // A final linearizable read of every key, then check every replica converged to the same data.
            Map<String, String> finalValues = finalReads(cluster, history, clock);
            String divergence = checkReplicasConverged(cluster, finalValues);

            List<Op> ops = new ArrayList<>(history);
            List<Result> results = LinearizabilityChecker.checkAll(ops, Duration.ofSeconds(120));
            String report = report(seed, seconds, readMode, nemesis, clients, ops, results, writesAfterHeal, divergence, cluster);
            Files.createDirectories(REPORT_DIR);
            Files.writeString(REPORT_DIR.resolve("report.txt"), report);
            Files.write(REPORT_DIR.resolve("history.txt"), ops.stream()
                    .sorted(Comparator.comparingLong(Op::invokedAt)).map(Op::toString).toList());

            if (nemesis.failure() != null) throw new AssertionError("the nemesis crashed", nemesis.failure());
            assertTrue(unexpected.isEmpty(), "unexpected errors from the server: " + unexpected);
            for (Result r : results) {
                if (r.verdict() != Verdict.LINEARIZABLE) {
                    fail("seed " + seed + ": " + r.verdict() + "\n" + r.explanation() + "\nfull report: " + REPORT_DIR.resolve("report.txt"));
                }
            }
            assertTrue(writesAfterHeal > 0, "no write succeeded in the 5 s after the faults stopped\n" + report);
            assertNull(divergence, divergence);
            long okOps = ops.stream().filter(op -> op.outcome() == Outcome.OK).count();
            assertTrue(okOps > 200, "too few operations succeeded to mean anything: " + okOps);
        }
    }

    /**
     * The anomaly ReadIndex exists to prevent, reproduced on a real cluster and caught by the checker.
     * A leader is cut off; the others elect a new leader and accept a write. The old leader doesn't
     * know it was replaced: in LEADER read mode it answers from its own memory, which is stale. In the
     * default CONSISTENT mode it must first confirm its leadership with a majority, can't, and so never
     * answers with stale data.
     */
    @Test
    void aCutOffLeaderServesStaleReadsUnlessReadsGoThroughReadIndex() throws Exception {
        try (ChaosCluster cluster = new ChaosCluster(3, dir, List.of())) {
            cluster.startAll();
            int oldLeader = cluster.awaitLeader(20_000);
            long t0 = System.nanoTime();
            LongSupplier clock = () -> System.nanoTime() - t0;
            List<Op> history = new ArrayList<>();

            long start = clock.getAsLong();
            try (Jedis j = new Jedis("127.0.0.1", cluster.clientPort(oldLeader), 5000)) {
                j.set("k", "1");
            }
            history.add(new Op(1, "k", Type.WRITE, 1L, start, clock.getAsLong(), Outcome.OK));

            List<Integer> others = new ArrayList<>(List.of(1, 2, 3));
            others.remove(Integer.valueOf(oldLeader));
            cluster.network.partition(List.of(Set.of(oldLeader), Set.copyOf(others)));
            int newLeader = -1;
            long deadline = System.currentTimeMillis() + 10_000;
            while ((newLeader <= 0 || newLeader == oldLeader) && System.currentTimeMillis() < deadline) {
                Thread.sleep(25);
                newLeader = cluster.leader();
            }
            assertNotEquals(oldLeader, newLeader, "the majority side never elected a new leader");

            start = clock.getAsLong();
            try (Jedis j = new Jedis("127.0.0.1", cluster.clientPort(newLeader), 5000)) {
                j.set("k", "2");
            }
            history.add(new Op(2, "k", Type.WRITE, 2L, start, clock.getAsLong(), Outcome.OK));

            // LEADER mode on the old leader: it still believes it leads, and answers from memory.
            start = clock.getAsLong();
            String stale;
            try (Jedis j = new Jedis("127.0.0.1", cluster.clientPort(oldLeader), 5000)) {
                readMode(j, "LEADER");
                stale = j.get("k");
            }
            history.add(new Op(3, "k", Type.READ, Long.parseLong(stale), start, clock.getAsLong(), Outcome.OK));
            assertEquals("1", stale, "the cut-off leader should still have the old value");

            Result result = LinearizabilityChecker.check("k", history, Duration.ofSeconds(10));
            assertEquals(Verdict.NOT_LINEARIZABLE, result.verdict(), "the checker must flag the stale read");
            assertTrue(result.explanation().contains("read  k -> 1"), result.explanation());

            // CONSISTENT mode (the default) on the same node: ReadIndex can't reach a majority, so no answer.
            try (Jedis j = new Jedis("127.0.0.1", cluster.clientPort(oldLeader), 1000)) {
                assertThrows(JedisConnectionException.class, () -> j.get("k"), "a consistent read must not return stale data");
            }

            // After healing, the old leader learns about the new term, steps down and catches up.
            cluster.network.healAll();
            deadline = System.currentTimeMillis() + 10_000;
            String caughtUp = null;
            while (!"2".equals(caughtUp) && System.currentTimeMillis() < deadline) {
                Thread.sleep(25);
                try (Jedis j = new Jedis("127.0.0.1", cluster.clientPort(oldLeader), 1000)) {
                    readMode(j, "STALE");
                    caughtUp = j.get("k");
                } catch (JedisException e) {
                    caughtUp = null;
                }
            }
            assertEquals("2", caughtUp, "the old leader never caught up after the partition healed");
        }
    }

    // ------------------------------------------------------------------ workload

    /** One client: picks a node, follows NOTLEADER redirects, and records every operation. */
    static final class Client implements Runnable {
        private final int id;
        private final ChaosCluster cluster;
        private final Random rnd;
        private final LongSupplier clock;
        private final Queue<Op> history;
        private final Queue<String> unexpected;
        private final String readMode;
        private final AtomicLong ok = new AtomicLong();
        private final AtomicLong unknown = new AtomicLong();
        private final AtomicLong failed = new AtomicLong();
        private volatile boolean stopping;
        private Jedis conn;
        private int node;
        private long writes;

        Client(int id, ChaosCluster cluster, Random rnd, LongSupplier clock, Queue<Op> history,
               Queue<String> unexpected, String readMode) {
            this.id = id;
            this.cluster = cluster;
            this.rnd = rnd;
            this.clock = clock;
            this.history = history;
            this.unexpected = unexpected;
            this.readMode = readMode;
            this.node = 1 + rnd.nextInt(cluster.size);
        }

        void stop() {
            stopping = true;
        }

        @Override
        public void run() {
            while (!stopping) {
                try {
                    if (connect()) step();
                    Thread.sleep(rnd.nextInt(10));
                } catch (InterruptedException e) {
                    return;
                }
            }
            disconnect();
        }

        /** Connects (and checks the node answers) before an operation starts, so a dead node isn't "maybe applied". */
        private boolean connect() throws InterruptedException {
            if (conn != null) return true;
            try {
                conn = new Jedis("127.0.0.1", cluster.clientPort(node), CLIENT_TIMEOUT_MS);
                conn.ping();
                if (!readMode.equals("CONSISTENT")) readMode(conn, readMode);
                return true;
            } catch (JedisException e) {
                disconnect();
                node = 1 + rnd.nextInt(cluster.size);
                Thread.sleep(20);
                return false;
            }
        }

        private void step() throws InterruptedException {
            // Now and then, reconnect to a random node, as a client behind a load balancer or with a
            // stale cached leader address would. Without this, clients settle on the current leader
            // and nobody ever talks to a deposed leader that is cut off, so its bugs would go unseen.
            if (rnd.nextInt(100) < 5) {
                switchTo(1 + rnd.nextInt(cluster.size));
                return;
            }
            String key = KEYS[rnd.nextInt(KEYS.length)];
            int dice = rnd.nextInt(100);
            Type type = dice < 50 ? Type.READ : dice < 85 ? Type.WRITE : Type.INCR;
            // Every written value is unique and 1,000,000 apart, so a value read tells us which write it came from.
            Long value = type == Type.WRITE ? id * 1_000_000_000_000L + (++writes) * 1_000_000L : null;
            long start = clock.getAsLong();
            try {
                Long result = switch (type) {
                    case READ -> {
                        String s = conn.get(key);
                        yield s == null ? null : Long.parseLong(s);
                    }
                    case WRITE -> {
                        conn.set(key, String.valueOf(value));
                        yield value;
                    }
                    case INCR -> conn.incr(key);
                };
                history.add(new Op(id, key, type, result, start, clock.getAsLong(), Outcome.OK));
                ok.incrementAndGet();
            } catch (JedisDataException e) {
                String msg = e.getMessage();
                if (msg.startsWith("NOTLEADER ")) {
                    failed.incrementAndGet(); // never proposed
                    int leader = cluster.nodeForPort(Integer.parseInt(msg.substring(msg.lastIndexOf(':') + 1)));
                    if (leader > 0) switchTo(leader);
                } else if (msg.startsWith("CLUSTERDOWN") || msg.startsWith("ERR write was not applied")) {
                    failed.incrementAndGet(); // definitely not applied
                    switchTo(1 + rnd.nextInt(cluster.size));
                    Thread.sleep(50);
                } else {
                    if (!msg.startsWith("UNCERTAIN")) unexpected.add(type + " " + key + ": " + msg);
                    unknownOutcome(type, key, value, start);
                }
            } catch (JedisConnectionException e) {
                // Timed out or the connection died: the server may or may not have applied it.
                unknownOutcome(type, key, value, start);
                switchTo(1 + rnd.nextInt(cluster.size));
            }
        }

        private void unknownOutcome(Type type, String key, Long value, long start) {
            unknown.incrementAndGet();
            history.add(new Op(id, key, type, type == Type.WRITE ? value : null, start, clock.getAsLong(), Outcome.UNKNOWN));
        }

        private void switchTo(int target) {
            disconnect();
            node = target;
        }

        private void disconnect() {
            if (conn != null) {
                try {
                    conn.close();
                } catch (RuntimeException ignored) {
                    // already broken
                }
            }
            conn = null;
        }
    }

    // ------------------------------------------------------------------ final checks

    private static Map<String, String> finalReads(ChaosCluster cluster, Queue<Op> history, LongSupplier clock) throws InterruptedException {
        Map<String, String> values = new TreeMap<>();
        long deadline = System.currentTimeMillis() + 30_000;
        for (String key : KEYS) {
            while (true) {
                if (System.currentTimeMillis() > deadline) throw new AssertionError("final reads failed for 30 s: no stable leader");
                int leader = cluster.leader();
                if (leader > 0) {
                    long start = clock.getAsLong();
                    try (Jedis j = new Jedis("127.0.0.1", cluster.clientPort(leader), 2000)) {
                        String v = j.get(key);
                        history.add(new Op(0, key, Type.READ, v == null ? null : Long.parseLong(v), start, clock.getAsLong(), Outcome.OK));
                        values.put(key, v);
                        break;
                    } catch (JedisException e) {
                        // leadership moved; try again
                    }
                }
                Thread.sleep(50);
            }
        }
        return values;
    }

    /** Returns null if every node ends up with exactly the final values, otherwise what differs. */
    private static String checkReplicasConverged(ChaosCluster cluster, Map<String, String> expected) throws InterruptedException {
        String last = null;
        long deadline = System.currentTimeMillis() + 20_000;
        while (System.currentTimeMillis() < deadline) {
            last = null;
            for (int id = 1; id <= cluster.size && last == null; id++) {
                try (Jedis j = new Jedis("127.0.0.1", cluster.clientPort(id), 2000)) {
                    readMode(j, "STALE");
                    for (Map.Entry<String, String> e : expected.entrySet()) {
                        String v = j.get(e.getKey());
                        if (!Objects.equals(v, e.getValue())) {
                            last = "node " + id + " has " + e.getKey() + " = " + v + " but the cluster's value is " + e.getValue();
                            break;
                        }
                    }
                } catch (JedisException e) {
                    last = "node " + id + " is not answering: " + e.getMessage();
                }
            }
            if (last == null) return null;
            Thread.sleep(100);
        }
        return last;
    }

    private static String report(long seed, int seconds, String readMode, Nemesis nemesis, List<Client> clients, List<Op> ops,
                                 List<Result> results, long writesAfterHeal, String divergence, ChaosCluster cluster) {
        StringBuilder sb = new StringBuilder();
        sb.append("raftis chaos test\n=================\n");
        sb.append(String.format("seed %d, %d s of faults, %d nodes, %d clients, %d keys, read mode %s%n%n",
                seed, seconds, NODES, CLIENTS, KEYS.length, readMode));
        sb.append("operations:\n");
        for (Type t : Type.values()) {
            long okN = ops.stream().filter(o -> o.type() == t && o.outcome() == Outcome.OK).count();
            long unk = ops.stream().filter(o -> o.type() == t && o.outcome() == Outcome.UNKNOWN).count();
            sb.append(String.format("  %-6s %7d ok  %6d no answer (outcome unknown)%n", t, okN, unk));
        }
        long failed = clients.stream().mapToLong(c -> c.failed.get()).sum();
        sb.append(String.format("  plus %d refused with a definite error (NOTLEADER, CLUSTERDOWN, not applied)%n", failed));
        sb.append(String.format("  writes that succeeded in the 5 s after the last fault: %d%n%n", writesAfterHeal));

        sb.append("faults injected:\n");
        nemesis.counts().forEach((f, n) -> sb.append(String.format("  %-15s %d%n", f, n)));
        sb.append("\nnodes at the end (counters are since each node's last restart):\n");
        for (int id = 1; id <= cluster.size; id++) {
            String info = cluster.info(id);
            sb.append(String.format("  node %d: %s%n", id, info == null ? "not answering" : String.format(
                    "%-8s term %-3s commit index %-6s snapshot index %-6s elections started %-3s snapshots sent %-3s installed %s",
                    ChaosCluster.field(info, "role"), ChaosCluster.field(info, "term"),
                    ChaosCluster.field(info, "commit_index"), ChaosCluster.field(info, "snapshot_index"),
                    ChaosCluster.field(info, "elections_started"), ChaosCluster.field(info, "snapshots_sent"),
                    ChaosCluster.field(info, "snapshots_installed"))));
        }
        sb.append("\nlinearizability, checked per key:\n");
        for (Result r : results) {
            sb.append(String.format("  %-3s %-17s %6d operations  %5d ms%n", r.key(), r.verdict(), r.operations(), r.millis()));
            if (!r.explanation().isEmpty()) sb.append(r.explanation().indent(6));
        }
        sb.append("\nreplicas identical at the end: ").append(divergence == null ? "yes" : "NO: " + divergence).append("\n\n");
        sb.append("timeline:\n");
        nemesis.log().forEach(line -> sb.append("  ").append(line).append('\n'));
        return sb.toString();
    }

    private static void readMode(Jedis j, String mode) {
        j.sendCommand(() -> "READMODE".getBytes(StandardCharsets.US_ASCII), mode);
    }
}
