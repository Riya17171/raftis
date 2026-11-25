package com.raftis.persistence;

import com.raftis.server.Config;
import com.raftis.server.Server;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.SetParams;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Restart the server on the same data directory and check every data type comes back. */
class PersistenceTest {
    @TempDir
    Path dir;

    private Server start(boolean aof) throws IOException {
        Server s = new Server(new Config("127.0.0.1", 0, dir, aof, FsyncPolicy.ALWAYS));
        s.start();
        s.runInBackground();
        return s;
    }

    private static void writeSampleData(Jedis j) {
        j.set("name", "raftis");
        j.set("session", "abc", SetParams.setParams().ex(1000));
        j.set("short-lived", "x", SetParams.setParams().px(100));
        j.incrBy("counter", 41);
        j.incr("counter");
        j.rpush("list", "a", "b", "c");
        j.lpop("list");
        j.hset("user", Map.of("name", "Riya", "lang", "java"));
        j.zadd("board", 1.5, "alice");
        j.zadd("board", 99, "bob");
        j.zincrby("board", 100, "alice");
    }

    private static void verifySampleData(Jedis j) {
        assertEquals("raftis", j.get("name"));
        long ttl = j.ttl("session");
        assertTrue(ttl > 990 && ttl <= 1000, "TTL must survive a restart as an absolute time; was " + ttl);
        assertNull(j.get("short-lived"), "a key whose TTL passed while the server was down must be gone");
        assertEquals("42", j.get("counter"));
        assertEquals(List.of("b", "c"), j.lrange("list", 0, -1));
        assertEquals(Map.of("name", "Riya", "lang", "java"), j.hgetAll("user"));
        assertEquals(List.of("bob", "alice"), j.zrange("board", 0, -1));
        assertEquals(101.5, j.zscore("board", "alice"));
    }

    @Test
    void everythingSurvivesARestartThroughTheAof() throws Exception {
        Server s = start(true);
        try (Jedis j = new Jedis("127.0.0.1", s.port())) {
            writeSampleData(j);
        }
        s.shutdown();
        Thread.sleep(150);

        Server s2 = start(true);
        try (Jedis j = new Jedis("127.0.0.1", s2.port())) {
            verifySampleData(j);
        } finally {
            s2.shutdown();
        }
    }

    @Test
    void snapshotPlusNewerAofEntriesAreBothRecovered() throws Exception {
        Server s = start(true);
        try (Jedis j = new Jedis("127.0.0.1", s.port())) {
            j.set("before-save", "1");
            j.incr("counter");
            assertEquals("OK", j.save());
            j.set("after-save", "2");
            j.incr("counter"); // must not be applied twice after restart
        }
        s.shutdown();
        assertEquals(List.of("appendonly.2.aof"), aofFiles(), "SAVE starts a new AOF generation and deletes the old one");

        Server s2 = start(true);
        try (Jedis j = new Jedis("127.0.0.1", s2.port())) {
            assertEquals("1", j.get("before-save"));
            assertEquals("2", j.get("after-save"));
            assertEquals("2", j.get("counter"));
        } finally {
            s2.shutdown();
        }
    }

    @Test
    void withoutAofTheServerSnapshotsOnShutdown() throws Exception {
        Server s = start(false);
        try (Jedis j = new Jedis("127.0.0.1", s.port())) {
            writeSampleData(j);
        }
        s.shutdown();
        Thread.sleep(150);
        Server s2 = start(false);
        try (Jedis j = new Jedis("127.0.0.1", s2.port())) {
            verifySampleData(j);
        } finally {
            s2.shutdown();
        }
    }

    @Test
    void aCommandCutOffByACrashIsDroppedAndTheFileRepaired() throws Exception {
        String complete = "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\n2\r\n";
        String partial = "*3\r\n$3\r\nSET\r\n$1\r\nc\r\n$1";
        Path aof = dir.resolve("appendonly.1.aof");
        Files.writeString(aof, complete + partial, StandardCharsets.US_ASCII);

        Server s = start(true);
        try (Jedis j = new Jedis("127.0.0.1", s.port())) {
            assertEquals("1", j.get("a"));
            assertEquals("2", j.get("b"));
            assertNull(j.get("c"));
        } finally {
            s.shutdown();
        }
        assertTrue(Files.readString(aof, StandardCharsets.US_ASCII).startsWith(complete));
    }

    @Test
    void aCorruptedSnapshotIsRefused() throws Exception {
        Server s = start(true);
        try (Jedis j = new Jedis("127.0.0.1", s.port())) {
            for (int i = 0; i < 100; i++) j.set("key" + i, "value" + i);
            j.save();
        }
        s.shutdown();

        Path snapshot = dir.resolve(Persistence.SNAPSHOT_FILE);
        byte[] bytes = Files.readAllBytes(snapshot);
        bytes[bytes.length / 2] ^= 0x01; // flip one bit
        Files.write(snapshot, bytes);

        Server broken = new Server(new Config("127.0.0.1", 0, dir, true, FsyncPolicy.ALWAYS));
        IOException e = assertThrows(IOException.class, broken::start);
        assertTrue(e.getMessage().contains("CRC32"), e.getMessage());
    }

    private List<String> aofFiles() throws IOException {
        try (Stream<Path> files = Files.list(dir)) {
            return files.map(p -> p.getFileName().toString()).filter(n -> n.endsWith(".aof")).sorted().toList();
        }
    }
}
