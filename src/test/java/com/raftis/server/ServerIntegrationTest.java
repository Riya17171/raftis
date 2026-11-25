package com.raftis.server;

import com.raftis.persistence.FsyncPolicy;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.params.SetParams;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Talks to the server through Jedis, a real Java Redis client library. If Jedis works
 * unmodified, the server really is Redis-compatible.
 */
class ServerIntegrationTest {
    @TempDir
    static Path dir;
    static Server server;
    static Jedis jedis;

    @BeforeAll
    static void start() throws Exception {
        server = new Server(new Config("127.0.0.1", 0, dir, true, FsyncPolicy.EVERYSEC));
        server.start();
        server.runInBackground();
        jedis = new Jedis("127.0.0.1", server.port());
    }

    @AfterAll
    static void stop() {
        jedis.close();
        server.shutdown();
    }

    @BeforeEach
    void flush() {
        jedis.flushAll();
    }

    @Test
    void strings() {
        assertEquals("PONG", jedis.ping());
        assertEquals("OK", jedis.set("name", "raftis"));
        assertEquals("raftis", jedis.get("name"));
        assertNull(jedis.get("missing"));
        assertNull(jedis.set("name", "other", SetParams.setParams().nx()), "NX must not overwrite");
        assertEquals("OK", jedis.set("name", "other", SetParams.setParams().xx()));
        assertEquals(11L, jedis.incrBy("counter", 11));
        assertEquals(10L, jedis.decr("counter"));
        assertEquals(6L, jedis.append("greet", "hello!"));
        assertEquals(6L, jedis.strlen("greet"));
        jedis.mset("a", "1", "b", "2");
        assertEquals(Arrays.asList("1", "2", null), jedis.mget("a", "b", "nope"));
        JedisDataException e = assertThrows(JedisDataException.class, () -> jedis.incr("name"));
        assertEquals("ERR value is not an integer or out of range", e.getMessage());
    }

    @Test
    void expiry() throws Exception {
        jedis.set("temp", "x", SetParams.setParams().px(150));
        long pttl = jedis.pttl("temp");
        assertTrue(pttl > 0 && pttl <= 150, "pttl was " + pttl);
        jedis.set("forever", "x");
        assertEquals(-1L, jedis.ttl("forever"));
        assertEquals(-2L, jedis.ttl("nope"));
        Thread.sleep(200);
        assertNull(jedis.get("temp"));

        jedis.set("e", "x");
        assertEquals(1L, jedis.expire("e", 100));
        assertTrue(jedis.ttl("e") > 90);
        assertEquals(1L, jedis.persist("e"));
        assertEquals(-1L, jedis.ttl("e"));
        assertEquals(0L, jedis.expire("nope", 100));
    }

    @Test
    void lists() {
        assertEquals(3L, jedis.rpush("l", "b", "c", "d"));
        assertEquals(4L, jedis.lpush("l", "a"));
        assertEquals(List.of("a", "b", "c", "d"), jedis.lrange("l", 0, -1));
        assertEquals(List.of("c", "d"), jedis.lrange("l", -2, 100));
        assertEquals("d", jedis.lindex("l", -1));
        assertEquals("a", jedis.lpop("l"));
        assertEquals("d", jedis.rpop("l"));
        assertEquals(2L, jedis.llen("l"));
        jedis.lpop("l");
        jedis.lpop("l");
        assertFalse(jedis.exists("l"), "empty lists are deleted");
    }

    @Test
    void listsGrowPastTheirInitialCapacity() {
        List<String> expected = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            if (i % 2 == 0) {
                jedis.rpush("big", "r" + i);
                expected.add("r" + i);
            } else {
                jedis.lpush("big", "l" + i);
                expected.add(0, "l" + i);
            }
        }
        assertEquals(expected, jedis.lrange("big", 0, -1));
    }

    @Test
    void hashes() {
        assertEquals(2L, jedis.hset("h", Map.of("f1", "v1", "f2", "v2")));
        assertEquals("v1", jedis.hget("h", "f1"));
        assertEquals(Map.of("f1", "v1", "f2", "v2"), jedis.hgetAll("h"));
        assertEquals(5L, jedis.hincrBy("h", "n", 5));
        assertTrue(jedis.hexists("h", "n"));
        assertEquals(3L, jedis.hlen("h"));
        assertEquals(1L, jedis.hdel("h", "f1"));
        assertEquals("hash", jedis.type("h"));
    }

    @Test
    void sortedSets() {
        jedis.zadd("z", 3, "c");
        jedis.zadd("z", 1, "a");
        jedis.zadd("z", 2, "b");
        assertEquals(List.of("a", "b", "c"), jedis.zrange("z", 0, -1));
        assertEquals(2L, jedis.zrank("z", "c"));
        assertEquals(1.0, jedis.zscore("z", "a"));
        assertEquals(5.5, jedis.zincrby("z", 4.5, "a"));
        assertEquals(List.of("b", "c", "a"), jedis.zrange("z", 0, -1));
        assertEquals(List.of("b", "c"), jedis.zrangeByScore("z", 2, 3));
        assertEquals(List.of("c"), jedis.zrangeByScore("z", "(2", "3"));
        assertEquals(1L, jedis.zrem("z", "b"));
        assertEquals(2L, jedis.zcard("z"));
        assertNull(jedis.zrank("z", "nope"));
    }

    @Test
    void keysTypesAndErrors() {
        jedis.set("user:1", "a");
        jedis.set("user:2", "b");
        jedis.set("other", "c");
        assertEquals(Set.of("user:1", "user:2"), jedis.keys("user:*"));
        assertEquals(3L, jedis.dbSize());
        assertEquals("string", jedis.type("other"));
        assertEquals("none", jedis.type("nope"));
        JedisDataException e = assertThrows(JedisDataException.class, () -> jedis.lpush("other", "x"));
        assertTrue(e.getMessage().startsWith("WRONGTYPE"));
        JedisDataException unknown = assertThrows(JedisDataException.class,
                () -> jedis.sendCommand(() -> "NOPE".getBytes(StandardCharsets.UTF_8)));
        assertEquals("ERR unknown command 'NOPE'", unknown.getMessage());
        assertEquals(2L, jedis.del("user:1", "user:2", "nope"));
    }

    @Test
    void pubSubDeliversMessages() throws Exception {
        CountDownLatch subscribed = new CountDownLatch(1);
        BlockingQueue<String> received = new LinkedBlockingQueue<>();
        JedisPubSub listener = new JedisPubSub() {
            @Override
            public void onSubscribe(String channel, int count) {
                subscribed.countDown();
            }

            @Override
            public void onMessage(String channel, String message) {
                received.add(channel + ":" + message);
                unsubscribe();
            }
        };
        Thread subscriber = new Thread(() -> {
            try (Jedis sub = new Jedis("127.0.0.1", server.port())) {
                sub.subscribe(listener, "news");
            }
        });
        subscriber.start();
        assertTrue(subscribed.await(5, TimeUnit.SECONDS));
        assertEquals(1L, jedis.publish("news", "hello"));
        assertEquals("news:hello", received.poll(5, TimeUnit.SECONDS));
        subscriber.join(5000);
        assertEquals(0L, jedis.publish("news", "nobody listening"));
    }
}
