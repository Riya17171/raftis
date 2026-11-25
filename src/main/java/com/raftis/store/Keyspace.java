package com.raftis.store;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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
 *
 * <p><b>Cluster mode</b> adds two switches, because every node must make identical changes:
 * <ul>
 *   <li>{@link #setFixedTime}: while applying a Raft log entry, "now" is the timestamp the leader
 *       put in that entry, not this machine's clock, so every node makes the same expiry decisions.</li>
 *   <li>{@link #setReadOnly}: while serving a read, an expired key is reported missing but not
 *       deleted. Deleting it would change this node's data outside the log.</li>
 * </ul>
 */
public final class Keyspace {
    private static final int SAMPLE_SIZE = 20;
    private static final long CYCLE_BUDGET_NANOS = 25_000_000L;

    private final HashMap<ByteKey, Object> data = new HashMap<>();
    private final ExpiryIndex expires = new ExpiryIndex();
    private final LongSupplier clock;
    private final Random random = new Random();
    private boolean loading;
    private boolean readOnly;
    private long fixedTime = -1;
    private long expiredKeys;

    public Keyspace(LongSupplier clock) {
        this.clock = clock;
    }

    public long now() {
        return fixedTime >= 0 ? fixedTime : clock.getAsLong();
    }

    /** Looks up a key. An expired key is deleted (unless read-only) and reported missing. */
    public Object get(ByteKey key) {
        if (expireIfNeeded(key)) return null;
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

    /** Makes {@link #now()} return {@code time} (a Raft entry's timestamp) until {@link #clearFixedTime()}. */
    public void setFixedTime(long time) {
        this.fixedTime = time;
    }

    public void clearFixedTime() {
        this.fixedTime = -1;
    }

    /** While read-only, expired keys look missing but are not deleted. */
    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    /**
     * Cluster-mode active expiry: finds keys whose TTL has passed, without deleting them. The
     * leader then deletes them through the Raft log so every node deletes the same keys.
     */
    public List<ByteKey> sampleExpiredKeys(int max) {
        List<ByteKey> found = new ArrayList<>();
        Set<ByteKey> seen = new HashSet<>();
        long now = now();
        for (int round = 0; round < 16 && expires.size() > 0 && found.size() < max; round++) {
            int sample = Math.min(SAMPLE_SIZE, expires.size());
            int expired = 0;
            for (int i = 0; i < sample; i++) {
                ExpiryIndex.Entry e = expires.random(random);
                if (e.at <= now) {
                    expired++;
                    if (seen.add(e.key)) found.add(e.key);
                }
            }
            if (expired * 4 <= sample) break;
        }
        return found;
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

    /** Returns true if the key has expired, deleting it unless read-only. */
    private boolean expireIfNeeded(ByteKey key) {
        if (!isExpired(key)) return false;
        if (!readOnly) {
            removeKey(key);
            expiredKeys++;
        }
        return true;
    }

    private boolean removeKey(ByteKey key) {
        expires.remove(key);
        return data.remove(key) != null;
    }
}
