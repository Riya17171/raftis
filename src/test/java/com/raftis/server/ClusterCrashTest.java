package com.raftis.server;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Three real server processes. A client INCRs a counter on the leader as fast as it can; we
 * SIGKILL the leader at a random moment, restart it, and repeat. At the end, the counter must
 * account for every acknowledged INCR.
 *
 * <p>An INCR that got no answer (the leader died, or it said UNCERTAIN) may or may not have been
 * applied. So the final value must be at least the last acknowledged value, and at most that plus
 * the number of unanswered INCRs.
 *
 * <p>More rounds: {@code ./gradlew test --tests '*ClusterCrashTest*' -Draftis.clusterCrashRuns=50}
 */
class ClusterCrashTest {
    @TempDir
    Path dir;
    private final Process[] processes = new Process[4];
    private final int[] ports = new int[4];
    private String members;

    @Test
    void killingTheLeaderNeverLosesAnAcknowledgedWrite() throws Exception {
        StringBuilder sb = new StringBuilder();
        for (int id = 1; id <= 3; id++) {
            ports[id] = ClusterIntegrationTest.freePort();
            if (sb.length() > 0) sb.append(',');
            sb.append(id).append("=127.0.0.1:").append(ports[id]).append(':').append(ClusterIntegrationTest.freePort());
        }
        members = sb.toString();
        for (int id = 1; id <= 3; id++) start(id);

        int rounds = Integer.getInteger("raftis.clusterCrashRuns", 5);
        Random rnd = new Random();
        AtomicLong lastAcked = new AtomicLong();
        AtomicLong unanswered = new AtomicLong();
        try {
            for (int round = 1; round <= rounds; round++) {
                int leader = waitForLeader();
                Thread writer = new Thread(() -> {
                    try (Jedis j = new Jedis("127.0.0.1", ports[leader], 2000)) {
                        while (true) {
                            try {
                                lastAcked.set(j.incr("counter"));
                            } catch (JedisDataException e) {
                                // NOTLEADER / CLUSTERDOWN: definitely not applied. UNCERTAIN: maybe applied.
                                if (e.getMessage().startsWith("UNCERTAIN")) unanswered.incrementAndGet();
                                return;
                            }
                        }
                    } catch (RuntimeException connectionLost) {
                        unanswered.incrementAndGet(); // the INCR in flight when the leader died
                    }
                });
                writer.start();
                Thread.sleep(300 + rnd.nextInt(500));
                processes[leader].destroyForcibly(); // SIGKILL
                processes[leader].waitFor();
                writer.join(10_000);
                start(leader); // it rejoins as a follower and catches up
            }

            try (Jedis j = new Jedis("127.0.0.1", ports[waitForLeader()], 5000)) {
                long value = Long.parseLong(j.get("counter"));
                long acked = lastAcked.get();
                assertTrue(acked > 0, "the test never got a write through");
                assertTrue(value >= acked && value <= acked + unanswered.get(),
                        "counter is " + value + " but " + acked + " increments were acknowledged and "
                                + unanswered.get() + " got no answer");
            }
        } finally {
            for (int id = 1; id <= 3; id++) {
                if (processes[id] != null) {
                    processes[id].destroyForcibly();
                    processes[id].waitFor();
                }
            }
        }
    }

    private void start(int id) throws IOException {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        String classpath = System.getProperty("raftis.serverClasspath", System.getProperty("java.class.path"));
        ProcessBuilder pb = new ProcessBuilder(java, "-cp", classpath, "com.raftis.Main",
                "--node-id", String.valueOf(id), "--cluster", members, "--dir", dir.resolve("node" + id).toString());
        pb.redirectErrorStream(true);
        pb.redirectOutput(ProcessBuilder.Redirect.appendTo(dir.resolve("node" + id + ".log").toFile()));
        processes[id] = pb.start();
    }

    private int waitForLeader() throws InterruptedException {
        long deadline = System.currentTimeMillis() + 20_000;
        while (System.currentTimeMillis() < deadline) {
            for (int id = 1; id <= 3; id++) {
                if (processes[id] == null || !processes[id].isAlive()) continue;
                try (Jedis j = new Jedis("127.0.0.1", ports[id], 1000)) {
                    if (j.info().contains("role:leader")) return id;
                } catch (RuntimeException ignored) {
                    // not up yet
                }
            }
            Thread.sleep(25);
        }
        throw new AssertionError("no leader elected within 20 s");
    }
}
