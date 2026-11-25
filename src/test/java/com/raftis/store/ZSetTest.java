package com.raftis.store;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Property-style test: run 50,000 random operations against the skip list and against a
 * simple, obviously-correct reference (a HashMap that gets sorted), and check they agree.
 * Scores are small integers so there are lots of ties, which exercises member ordering.
 */
class ZSetTest {

    @Test
    void agreesWithAReferenceImplementationUnderRandomOperations() {
        Random rnd = new Random(42);
        ZSet zset = new ZSet();
        Map<String, Double> reference = new HashMap<>();

        for (int op = 0; op < 50_000; op++) {
            String member = "m" + rnd.nextInt(500);
            byte[] mb = member.getBytes(StandardCharsets.UTF_8);
            int kind = rnd.nextInt(10);
            if (kind < 6) {
                double score = rnd.nextInt(100);
                assertEquals(!reference.containsKey(member), zset.add(mb, score));
                reference.put(member, score);
            } else if (kind < 9) {
                assertEquals(reference.remove(member) != null, zset.remove(mb));
            } else {
                assertEquals(reference.get(member), zset.score(mb));
            }
            if (op % 500 == 0) assertSameAsReference(zset, reference, rnd);
        }
        assertSameAsReference(zset, reference, rnd);
    }

    @Test
    void handlesInfinityAndUpdates() {
        ZSet z = new ZSet();
        z.add(b("a"), Double.POSITIVE_INFINITY);
        z.add(b("b"), Double.NEGATIVE_INFINITY);
        z.add(b("c"), 0);
        assertEquals(List.of("b", "c", "a"), names(z.rangeByRank(0, 2)));
        z.add(b("b"), 10); // move b to the end
        assertEquals(List.of("c", "b", "a"), names(z.rangeByRank(0, 2)));
        assertEquals(1, z.rank(b("b")));
    }

    private static void assertSameAsReference(ZSet zset, Map<String, Double> reference, Random rnd) {
        List<String> expected = reference.entrySet().stream()
                .sorted((x, y) -> {
                    int c = Double.compare(x.getValue(), y.getValue());
                    return c != 0 ? c : x.getKey().compareTo(y.getKey());
                })
                .map(Map.Entry::getKey)
                .toList();

        assertEquals(expected.size(), zset.size());
        assertEquals(expected, names(zset.rangeByRank(0, zset.size() - 1)));
        for (int i = 0; i < expected.size(); i++) {
            assertEquals(i, zset.rank(b(expected.get(i))), "rank of " + expected.get(i));
        }

        double lo = rnd.nextInt(100);
        double hi = lo + rnd.nextInt(30);
        List<String> inRange = expected.stream()
                .filter(m -> reference.get(m) > lo && reference.get(m) <= hi)
                .toList();
        assertEquals(inRange, names(zset.rangeByScore(lo, true, hi, false, 0, -1)));
    }

    private static List<String> names(List<ZSet.Node> nodes) {
        return nodes.stream().map(n -> new String(n.member(), StandardCharsets.UTF_8)).toList();
    }

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
