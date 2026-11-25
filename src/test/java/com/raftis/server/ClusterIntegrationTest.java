package com.raftis.server;

import com.raftis.persistence.FsyncPolicy;
import com.raftis.raft.ClusterConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.params.SetParams;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A real 3-node cluster in one JVM, driven through Jedis, over real TCP and real disk. */
class ClusterIntegrationTest {
    @TempDir
    Path dir;
    private final Server[] nodes = new Server[4]; // index = node id; 0 unused
    private final int[] clientPorts = new int[4];
    private String members;

    @BeforeEach
    void startCluster() throws IOException {
        StringBuilder sb = new StringBuilder();
        for (int id = 1; id <= 3; id++) {
            clientPorts[id] = freePort();
            if (sb.length() > 0) sb.append(',');
            sb.append(id).append("=127.0.0.1:").append(clientPorts[id]).append(':').append(freePort());
        }
        members = sb.toString();
        for (int id = 1; id <= 3; id++) start(id);
    }

    @AfterEach
    void stopCluster() {
        for (int id = 1; id <= 3; id++) stop(id);
    }

    @Test
    void replicatesWritesAndRedirectsClients() {
        int leader = waitForLeader();
        int follower = leader == 1 ? 2 : 1;
        try (Jedis l = client(leader); Jedis f = client(follower)) {
            assertEquals("OK", l.set("name", "raftis"));
            assertEquals(42L, l.incrBy("counter", 42));
            l.rpush("list", "a", "b");
            l.hset("user", Map.of("lang", "java"));
            l.zadd("board", 5, "alice");
            assertEquals("42", l.get("counter"), "consistent read on the leader");

            JedisDataException e = assertThrows(JedisDataException.class, () -> f.get("counter"));
            assertEquals("NOTLEADER 127.0.0.1:" + clientPorts[leader], e.getMessage());
            assertThrows(JedisDataException.class, () -> f.set("x", "1"));

            readMode(f, "STALE");
            eventually(() -> "42".equals(f.get("counter")), "follower never caught up");
            assertEquals(List.of("a", "b"), f.lrange("list", 0, -1));
            assertEquals("java", f.hget("user", "lang"));
            assertEquals(5.0, f.zscore("board", "alice"));
        }
    }

    @Test
    void survivesLosingTheLeaderAndTheOldLeaderCatchesUp() {
        int oldLeader = waitForLeader();
        try (Jedis l = client(oldLeader)) {
            for (int i = 0; i < 100; i++) l.incr("counter");
        }
        stop(oldLeader);
        int newLeader = waitForLeader();
        assertNotEquals(oldLeader, newLeader);
        try (Jedis l = client(newLeader)) {
            assertEquals("100", l.get("counter"), "acknowledged writes must survive losing the leader");
            assertEquals(101L, l.incr("counter"));
        }
        start(oldLeader);
        try (Jedis back = client(oldLeader)) {
            readMode(back, "STALE");
            eventually(() -> "101".equals(back.get("counter")), "restarted node never caught up");
        }
    }

    @Test
    void keysExpireOnEveryNodeThroughTheLog() {
        int leader = waitForLeader();
        try (Jedis l = client(leader)) {
            l.set("temp", "x", SetParams.setParams().px(300));
            l.set("keep", "y");
        }
        for (int id = 1; id <= 3; id++) {
            try (Jedis n = client(id)) {
                readMode(n, "STALE");
                eventually(() -> "y".equals(n.get("keep")), "node " + id + " never got the keys");
                // Deleted for real (DBSIZE drops to 1), not just hidden: the leader's expiry went through the log.
                eventually(() -> n.dbSize() == 1, "node " + id + " never deleted the expired key");
                assertNull(n.get("temp"));
            }
        }
    }

    @Test
    void aFollowerThatMissedCompactedEntriesCatchesUpFromASnapshot() {
        int leader = waitForLeader();
        int lagging = leader == 3 ? 2 : 3;
        stop(lagging);
        try (Jedis l = client(leader)) {
            for (int i = 0; i < 300; i++) l.set("key" + i, "value" + i);
            assertEquals("OK", l.save()); // snapshot + drop the log the lagging node would need
            for (int i = 300; i < 310; i++) l.set("key" + i, "value" + i);
        }
        start(lagging);
        try (Jedis n = client(lagging)) {
            readMode(n, "STALE");
            eventually(() -> n.dbSize() == 310, "lagging node never caught up through InstallSnapshot");
            assertEquals("value0", n.get("key0"));
            assertEquals("value309", n.get("key309"));
            String info = n.info();
            assertTrue(info.contains("snapshot_index:") && !info.contains("snapshot_index:0\r"), info);
        }
    }

    @Test
    void dataSurvivesRestartingTheWholeCluster() {
        int leader = waitForLeader();
        try (Jedis l = client(leader)) {
            for (int i = 0; i < 50; i++) l.incr("counter");
            l.save();
            for (int i = 0; i < 50; i++) l.incr("counter");
        }
        for (int id = 1; id <= 3; id++) stop(id);
        for (int id = 1; id <= 3; id++) start(id);
        try (Jedis l = client(waitForLeader())) {
            assertEquals("100", l.get("counter"));
        }
    }

    // ------------------------------------------------------------------ helpers

    private void start(int id) {
        try {
            Config config = new Config("127.0.0.1", 0, dir.resolve("node" + id), true, FsyncPolicy.ALWAYS,
                    ClusterConfig.parse(id, members));
            nodes[id] = new Server(config);
            nodes[id].start();
            nodes[id].runInBackground();
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private void stop(int id) {
        if (nodes[id] != null) nodes[id].shutdown();
        nodes[id] = null;
    }

    private Jedis client(int id) {
        return new Jedis("127.0.0.1", clientPorts[id], 5000);
    }

    private int waitForLeader() {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            for (int id = 1; id <= 3; id++) {
                if (nodes[id] == null) continue;
                try (Jedis j = client(id)) {
                    if (j.info().contains("role:leader")) return id;
                } catch (RuntimeException ignored) {
                    // node still starting
                }
            }
            sleep(20);
        }
        throw new AssertionError("no leader elected within 10 s");
    }

    private static void readMode(Jedis j, String mode) {
        j.sendCommand(() -> "READMODE".getBytes(StandardCharsets.US_ASCII), mode);
    }

    private static void eventually(Supplier<Boolean> condition, String message) {
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            if (condition.get()) return;
            sleep(20);
        }
        throw new AssertionError(message);
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
