package com.raftis.store;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Uses a fake clock so time-based behaviour is tested instantly and deterministically. */
class KeyspaceTest {
    private final AtomicLong clock = new AtomicLong(1_000_000);
    private final Keyspace db = new Keyspace(clock::get);

    private static final byte[] V = "v".getBytes(StandardCharsets.UTF_8);

    @Test
    void keysDisappearExactlyWhenTheirTtlPasses() {
        ByteKey k = ByteKey.of("k");
        db.set(k, V);
        db.setExpireAt(k, clock.get() + 100);
        clock.addAndGet(99);
        assertNotNull(db.get(k));
        clock.addAndGet(1);
        assertNull(db.get(k));
        assertEquals(0, db.size());
    }

    @Test
    void activeExpiryDeletesKeysThatNobodyReads() {
        for (int i = 0; i < 10_000; i++) {
            ByteKey k = ByteKey.of("temp" + i);
            db.set(k, V);
            db.setExpireAt(k, clock.get() + 1000);
        }
        for (int i = 0; i < 100; i++) db.set(ByteKey.of("keep" + i), V);
        clock.addAndGet(1000);

        int cycles = 0;
        while (db.size() > 100 && cycles < 1000) {
            db.activeExpireCycle();
            cycles++;
        }
        assertEquals(100, db.size(), "only keys without a TTL should remain");
        assertEquals(0, db.expiresCount());
    }

    @Test
    void timeIsFrozenWhileLoadingFromDisk() {
        ByteKey k = ByteKey.of("k");
        db.setLoading(true);
        db.set(k, V);
        db.setExpireAt(k, clock.get() - 1); // already in the past
        assertNotNull(db.get(k), "replay must rebuild the exact state, including expired keys");
        assertEquals(0, db.activeExpireCycle());
        db.setLoading(false);
        assertNull(db.get(k));
    }

    @Test
    void setClearsTheTtlButPutKeepsIt() {
        ByteKey k = ByteKey.of("k");
        db.set(k, V);
        db.setExpireAt(k, clock.get() + 1000);
        db.put(k, V);
        assertEquals(clock.get() + 1000, db.expireAt(k));
        db.set(k, V);
        assertEquals(-1, db.expireAt(k));
    }

    @Test
    void deleteReportsWhetherALiveKeyWasRemoved() {
        ByteKey k = ByteKey.of("k");
        db.set(k, V);
        db.setExpireAt(k, clock.get() + 10);
        clock.addAndGet(10);
        assertFalse(db.delete(k), "an expired key counts as already gone");
        db.set(k, V);
        assertTrue(db.delete(k));
    }
}
