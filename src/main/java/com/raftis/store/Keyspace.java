package com.raftis.store;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.LongSupplier;

/**
 * The whole dataset: a map from key to value, plus expiry times.
 *
 * <p>Values are {@code byte[]} (string), {@link RList}, {@link RHash} or {@link ZSet}.
 *
 * <p>There are no locks. Only the event-loop thread ever touches the keyspace, which is the
 * same design Redis uses: commands run one at a time, so each one is atomic for free.
 *
 * <p>Keys expire in two ways, as in Redis:
 * <ul>
 *   <li><b>Lazily</b>: every lookup checks the key's TTL first and deletes it if it has passed.</li>
 *   <li><b>Actively</b>: {@link #activeExpireCycle()} samples random keys with a TTL, so keys
 *       that nobody reads still get deleted and don't waste memory.</li>
 * </ul>
 *
 * <p>While loading data from disk, time is frozen: nothing expires. Replaying the log must
 * rebuild exactly the state that existed, including keys whose TTL passed while the server was
 * down; they are expired normally once loading finishes. Redis does the same.
 */
public final class Keyspace {
    private static final int SAMPLE_SIZE = 20;
    private static final long CYCLE_BUDGET_NANOS = 25_000_000L;

    private final HashMap<ByteKey, Object> data = new HashMap<>();
    private final ExpiryIndex expires = new ExpiryIndex();
    private final LongSupplier clock;
    private final Random random = new Random();
    private boolean loading;
    private long expiredKeys;

    public Keyspace(LongSupplier clock) {
        this.clock = clock;
    }

    public long now() {
        return clock.getAsLong();
    }

    /** Looks up a key, deleting it first if its TTL has passed. */
    public Object get(ByteKey key) {
        expireIfNeeded(key);
        return data.get(key);
    }

    /** Stores a value and clears any TTL: the semantics of SET. */
    public void set(ByteKey key, Object value) {
        data.put(key, value);
        expires.remove(key);
    }

    /** Stores a value and keeps any existing TTL: used for in-place changes like INCR or APPEND. */
    public void put(ByteKey key, Object value) {
        data.put(key, value);
    }

    /** Deletes a key. Returns false if it didn't exist or had already expired. */
    public boolean delete(ByteKey key) {
        if (expireIfNeeded(key)) return false;
        return removeKey(key);
    }

    /** Absolute expiry time in ms since the epoch, or -1 if the key has no TTL. */
    public long expireAt(ByteKey key) {
        return expires.get(key);
    }

    public void setExpireAt(ByteKey key, long at) {
        if (!data.containsKey(key)) throw new IllegalStateException("can't set a TTL on a missing key: " + key);
        expires.set(key, at);
    }

    /** Removes a key's TTL. Returns false if it had none. */
    public boolean persist(ByteKey key) {
        return expires.remove(key);
    }

    /** Whether an absolute expiry time is already in the past. Always false while loading. */
    public boolean alreadyExpired(long at) {
        return !loading && at <= now();
    }

    /** Whether a key's TTL has passed. Doesn't delete it. */
    public boolean isExpired(ByteKey key) {
        long at = expires.get(key);
        return at >= 0 && alreadyExpired(at);
    }

    public int size() {
        return data.size();
    }

    public int expiresCount() {
        return expires.size();
    }

    public long expiredKeys() {
        return expiredKeys;
    }

    public void clear() {
        data.clear();
        expires.clear();
    }

    public Set<ByteKey> keys() {
        return Collections.unmodifiableSet(data.keySet());
    }

    public Set<Map.Entry<ByteKey, Object>> entries() {
        return Collections.unmodifiableMap(data).entrySet();
    }

    public void setLoading(boolean loading) {
        this.loading = loading;
    }

    /**
     * Redis's active expiry algorithm: sample 20 random keys that have a TTL and delete the
     * expired ones. If more than 25% were expired, there are probably many more, so repeat,
     * but never spend more than 25 ms per call so clients aren't kept waiting.
     *
     * @return how many keys were deleted
     */
    public int activeExpireCycle() {
        if (loading) return 0;
        long start = System.nanoTime();
        int total = 0;
        while (expires.size() > 0) {
            int sample = Math.min(SAMPLE_SIZE, expires.size());
            int expired = 0;
            long now = now();
            for (int i = 0; i < sample && expires.size() > 0; i++) {
                ExpiryIndex.Entry e = expires.random(random);
                if (e.at <= now) {
                    removeKey(e.key);
                    expired++;
                }
            }
            total += expired;
            if (expired * 4 <= sample) break;
            if (System.nanoTime() - start > CYCLE_BUDGET_NANOS) break;
        }
        expiredKeys += total;
        return total;
    }

    private boolean expireIfNeeded(ByteKey key) {
        if (!isExpired(key)) return false;
        removeKey(key);
        expiredKeys++;
        return true;
    }

    private boolean removeKey(ByteKey key) {
        expires.remove(key);
        return data.remove(key) != null;
    }
}
