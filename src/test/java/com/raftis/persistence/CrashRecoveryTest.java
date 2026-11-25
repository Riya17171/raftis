package com.raftis.persistence;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The durability promise, tested for real: start the server as a separate process, have a
 * client INCR a counter as fast as it can, kill the server with SIGKILL (kill -9) at a random
 * moment, restart it, and check that no acknowledged increment was lost.
 *
 * <p>With {@code appendfsync always} the recovered value must be the last acknowledged value,
 * or one more if the server crashed after logging an INCR but before replying to it.
 *
 * <p>Every third run also takes a snapshot (SAVE) while writes are happening, to test that
 * crashing around a snapshot never loses or double-applies writes.
 *
 * <p>Run more iterations with: {@code ./gradlew test -Draftis.crashRuns=100}
 */
class CrashRecoveryTest {
    @TempDir
    Path dataDir;
    @TempDir
    Path logDir;

    @Test
    void killDashNineNeverLosesAnAcknowledgedWrite() throws Exception {
        int runs = Integer.getInteger("raftis.crashRuns", 10);
        Random rnd = new Random();
        long lastAcked = 0;

        for (int run = 1; run <= runs; run++) {
            int port = freePort();
            Process server = startServer(port, run);
            try (Jedis j = connect(port)) {
                long recovered = readCounter(j);
                assertTrue(recovered >= lastAcked && recovered <= lastAcked + 1,
                        "run " + run + ": " + lastAcked + " increments were acknowledged but " + recovered + " survived");
                lastAcked = recovered;
            }

            AtomicLong acked = new AtomicLong(lastAcked);
            Thread writer = new Thread(() -> {
                try (Jedis j = new Jedis("127.0.0.1", port)) {
                    while (true) acked.set(j.incr("counter"));
                } catch (Exception expected) {
                    // the server was killed under us
                }
            });
            writer.start();

            if (run % 3 == 0) {
                Thread.sleep(50);
                try (Jedis j = new Jedis("127.0.0.1", port)) {
                    j.save();
                }
            }
            Thread.sleep(100 + rnd.nextInt(300));
            server.destroyForcibly(); // SIGKILL: no shutdown hook, no flushing, like a crash
            server.waitFor();
            writer.join(10_000);
            lastAcked = acked.get();
        }

        int port = freePort();
        Process server = startServer(port, runs + 1);
        try (Jedis j = connect(port)) {
            long recovered = readCounter(j);
            assertTrue(recovered >= lastAcked && recovered <= lastAcked + 1,
                    "final: " + lastAcked + " increments were acknowledged but " + recovered + " survived");
        } finally {
            server.destroyForcibly();
            server.waitFor();
        }
    }

    private Process startServer(int port, int run) throws IOException {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        String classpath = System.getProperty("raftis.serverClasspath", System.getProperty("java.class.path"));
        ProcessBuilder pb = new ProcessBuilder(java, "-cp", classpath, "com.raftis.Main",
                "--port", String.valueOf(port), "--dir", dataDir.toString(), "--appendfsync", "always");
        pb.redirectErrorStream(true);
        pb.redirectOutput(logDir.resolve("server-" + run + ".log").toFile());
        return pb.start();
    }

    private static Jedis connect(int port) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 20_000;
        while (true) {
            Jedis j = new Jedis("127.0.0.1", port);
            try {
                j.ping();
                return j;
            } catch (JedisConnectionException e) {
                j.close();
                if (System.currentTimeMillis() > deadline) throw e;
                Thread.sleep(50);
            }
        }
    }

    private static long readCounter(Jedis j) {
        String value = j.get("counter");
        return value == null ? 0 : Long.parseLong(value);
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
