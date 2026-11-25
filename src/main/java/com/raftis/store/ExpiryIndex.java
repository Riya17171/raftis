package com.raftis.store;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

/**
 * Expiry times for keys that have a TTL, with O(1) set, get, remove and random sampling.
 *
 * <p>Active expiry needs to pick random keys that have a TTL. A HashMap can't do that in O(1),
 * so entries also live in an ArrayList. Each entry remembers its position in the list;
 * removing swaps the last element into the hole, which keeps removal O(1).
 */
final class ExpiryIndex {
    static final class Entry {
        final ByteKey key;
        long at;
        int pos;

        Entry(ByteKey key, long at, int pos) {
            this.key = key;
            this.at = at;
            this.pos = pos;
        }
    }

    private final HashMap<ByteKey, Entry> byKey = new HashMap<>();
    private final ArrayList<Entry> list = new ArrayList<>();

    /** Absolute expiry time in ms, or -1 if the key has no TTL. */
    long get(ByteKey key) {
        Entry e = byKey.get(key);
        return e == null ? -1 : e.at;
    }

    void set(ByteKey key, long at) {
        Entry e = byKey.get(key);
        if (e != null) {
            e.at = at;
            return;
        }
        e = new Entry(key, at, list.size());
        list.add(e);
        byKey.put(key, e);
    }

    boolean remove(ByteKey key) {
        Entry e = byKey.remove(key);
        if (e == null) return false;
        Entry last = list.remove(list.size() - 1);
        if (last != e) {
            list.set(e.pos, last);
            last.pos = e.pos;
        }
        return true;
    }

    Entry random(Random random) {
        return list.get(random.nextInt(list.size()));
    }

    int size() {
        return list.size();
    }

    void clear() {
        byKey.clear();
        list.clear();
    }
}
