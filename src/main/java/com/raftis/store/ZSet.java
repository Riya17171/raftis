package com.raftis.store;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * The sorted-set type, built the way Redis builds it: a skip list plus a hash map.
 *
 * <ul>
 *   <li>The <b>hash map</b> gives O(1) ZSCORE.</li>
 *   <li>The <b>skip list</b> keeps members ordered by (score, member) for ranges.</li>
 * </ul>
 *
 * <p>Each forward pointer also stores a <b>span</b>: how many nodes it jumps over. Adding up
 * spans while walking down the levels gives a member's rank in O(log n). That is why this is
 * hand-written: {@code java.util.TreeMap} and {@code ConcurrentSkipListMap} can't find a
 * rank faster than O(n).
 *
 * <p>Like Redis: at most 32 levels, and each node gets an extra level with probability 1/4.
 */
public final class ZSet {
    private static final int MAX_LEVEL = 32;
    private static final double P = 0.25;

    public static final class Node {
        private final byte[] member;
        private final double score;
        private final Node[] forward;
        private final int[] span;
        private Node backward;

        Node(byte[] member, double score, int level) {
            this.member = member;
            this.score = score;
            this.forward = new Node[level];
            this.span = new int[level];
        }

        public byte[] member() {
            return member;
        }

        public double score() {
            return score;
        }

        public Node next() {
            return forward[0];
        }

        public Node previous() {
            return backward;
        }
    }

    private final Node header = new Node(null, 0, MAX_LEVEL);
    private final HashMap<ByteKey, Double> scores = new HashMap<>();
    private Node tail;
    private int level = 1;
    private int length;

    public int size() {
        return length;
    }

    public Double score(byte[] member) {
        return scores.get(new ByteKey(member));
    }

    /** Adds a member or changes its score. Returns true if the member is new. */
    public boolean add(byte[] member, double score) {
        Double old = scores.put(new ByteKey(member), score);
        if (old != null) {
            if (old == score) return false;
            delete(old, member);
        }
        insert(score, member);
        return old == null;
    }

    public boolean remove(byte[] member) {
        Double old = scores.remove(new ByteKey(member));
        if (old == null) return false;
        delete(old, member);
        return true;
    }

    /** 0-based rank in ascending order, or -1 if the member isn't in the set. O(log n). */
    public long rank(byte[] member) {
        Double score = score(member);
        if (score == null) return -1;
        long rank = 0;
        Node x = header;
        for (int i = level - 1; i >= 0; i--) {
            while (x.forward[i] != null && compare(x.forward[i], score, member) <= 0) {
                rank += x.span[i];
                x = x.forward[i];
            }
            if (x != header && Arrays.equals(x.member, member)) return rank - 1;
        }
        throw new IllegalStateException("skip list is out of sync with the score map");
    }

    /** The node at a 0-based rank, or null. O(log n). */
    public Node byRank(long rank) {
        long target = rank + 1;
        long traversed = 0;
        Node x = header;
        for (int i = level - 1; i >= 0; i--) {
            while (x.forward[i] != null && traversed + x.span[i] <= target) {
                traversed += x.span[i];
                x = x.forward[i];
            }
            if (traversed == target) return x;
        }
        return null;
    }

    /** Nodes with ranks in [start, stop]. The caller has already clamped both to valid ranks. */
    public List<Node> rangeByRank(long start, long stop) {
        List<Node> out = new ArrayList<>((int) Math.max(0, stop - start + 1));
        Node x = byRank(start);
        for (long i = start; i <= stop && x != null; i++, x = x.forward[0]) out.add(x);
        return out;
    }

    /**
     * Nodes whose score is within [min, max] (or exclusive bounds), skipping {@code offset}
     * matches and returning at most {@code limit} of them ({@code limit < 0} means no limit).
     */
    public List<Node> rangeByScore(double min, boolean minExclusive, double max, boolean maxExclusive,
                                   long offset, long limit) {
        Node x = header;
        for (int i = level - 1; i >= 0; i--) {
            while (x.forward[i] != null && !aboveMin(x.forward[i].score, min, minExclusive)) x = x.forward[i];
        }
        x = x.forward[0];
        while (x != null && offset > 0 && belowMax(x.score, max, maxExclusive)) {
            x = x.forward[0];
            offset--;
        }
        List<Node> out = new ArrayList<>();
        while (x != null && belowMax(x.score, max, maxExclusive) && (limit < 0 || out.size() < limit)) {
            out.add(x);
            x = x.forward[0];
        }
        return out;
    }

    /** The lowest-ranked node, for iterating in order with {@link Node#next()}. */
    public Node first() {
        return header.forward[0];
    }

    public Node last() {
        return tail;
    }

    private static boolean aboveMin(double score, double min, boolean exclusive) {
        return exclusive ? score > min : score >= min;
    }

    private static boolean belowMax(double score, double max, boolean exclusive) {
        return exclusive ? score < max : score <= max;
    }

    /** Orders by score, then by member bytes, exactly like Redis. */
    private static int compare(Node node, double score, byte[] member) {
        if (node.score < score) return -1;
        if (node.score > score) return 1;
        return Arrays.compareUnsigned(node.member, member);
    }

    private static int randomLevel() {
        int lvl = 1;
        while (lvl < MAX_LEVEL && ThreadLocalRandom.current().nextDouble() < P) lvl++;
        return lvl;
    }

    private void insert(double score, byte[] member) {
        Node[] update = new Node[MAX_LEVEL];
        long[] rank = new long[MAX_LEVEL];
        Node x = header;
        // Walk down from the top level, remembering the last node before the insert point
        // on each level (update[]) and its rank (rank[]), so spans can be fixed up afterwards.
        for (int i = level - 1; i >= 0; i--) {
            rank[i] = i == level - 1 ? 0 : rank[i + 1];
            while (x.forward[i] != null && compare(x.forward[i], score, member) < 0) {
                rank[i] += x.span[i];
                x = x.forward[i];
            }
            update[i] = x;
        }
        int newLevel = randomLevel();
        if (newLevel > level) {
            for (int i = level; i < newLevel; i++) {
                rank[i] = 0;
                update[i] = header;
                header.span[i] = length;
            }
            level = newLevel;
        }
        x = new Node(member, score, newLevel);
        for (int i = 0; i < newLevel; i++) {
            x.forward[i] = update[i].forward[i];
            update[i].forward[i] = x;
            x.span[i] = update[i].span[i] - (int) (rank[0] - rank[i]);
            update[i].span[i] = (int) (rank[0] - rank[i]) + 1;
        }
        // Levels above the new node now jump over one more node.
        for (int i = newLevel; i < level; i++) update[i].span[i]++;
        x.backward = update[0] == header ? null : update[0];
        if (x.forward[0] != null) x.forward[0].backward = x;
        else tail = x;
        length++;
    }

    private void delete(double score, byte[] member) {
        Node[] update = new Node[MAX_LEVEL];
        Node x = header;
        for (int i = level - 1; i >= 0; i--) {
            while (x.forward[i] != null && compare(x.forward[i], score, member) < 0) x = x.forward[i];
            update[i] = x;
        }
        x = x.forward[0];
        if (x == null || x.score != score || !Arrays.equals(x.member, member)) {
            throw new IllegalStateException("skip list is out of sync with the score map");
        }
        for (int i = 0; i < level; i++) {
            if (update[i].forward[i] == x) {
                update[i].span[i] += x.span[i] - 1;
                update[i].forward[i] = x.forward[i];
            } else {
                update[i].span[i]--;
            }
        }
        if (x.forward[0] != null) x.forward[0].backward = x.backward;
        else tail = x.backward;
        while (level > 1 && header.forward[level - 1] == null) level--;
        length--;
    }
}
