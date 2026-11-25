package com.raftis.chaos;

import com.raftis.chaos.LinearizabilityChecker.Verdict;
import com.raftis.chaos.Op.Outcome;
import com.raftis.chaos.Op.Type;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Known-good and known-bad histories, including the classic anomalies, so we know the checker can say no. */
class LinearizabilityCheckerTest {

    private static Op write(int client, long value, long from, long to) {
        return new Op(client, "k", Type.WRITE, value, from, to, Outcome.OK);
    }

    private static Op read(int client, Long value, long from, long to) {
        return new Op(client, "k", Type.READ, value, from, to, Outcome.OK);
    }

    private static Op incr(int client, Long result, long from, long to) {
        return new Op(client, "k", Type.INCR, result, from, to, Outcome.OK);
    }

    private static Op unknownWrite(int client, long value, long from) {
        return new Op(client, "k", Type.WRITE, value, from, from + 1000, Outcome.UNKNOWN);
    }

    private static Verdict verdict(Op... ops) {
        return LinearizabilityChecker.check("k", List.of(ops), Duration.ofSeconds(10)).verdict();
    }

    @Test
    void sequentialHistoryIsLinearizable() {
        assertEquals(Verdict.LINEARIZABLE, verdict(
                read(1, null, 0, 1), write(1, 5, 2, 3), read(2, 5L, 4, 5), incr(1, 6L, 6, 7), read(2, 6L, 8, 9)));
    }

    @Test
    void aReadOverlappingAWriteMaySeeEitherValue() {
        assertEquals(Verdict.LINEARIZABLE, verdict(write(1, 1, 0, 1), write(1, 2, 10, 20), read(2, 1L, 12, 14)));
        assertEquals(Verdict.LINEARIZABLE, verdict(write(1, 1, 0, 1), write(1, 2, 10, 20), read(2, 2L, 12, 14)));
    }

    @Test
    void aStaleReadIsCaught() {
        // The write of 2 finished before the read started, so the read must not return 1.
        assertEquals(Verdict.NOT_LINEARIZABLE, verdict(write(1, 1, 0, 1), write(1, 2, 2, 3), read(2, 1L, 4, 5)));
    }

    @Test
    void readingAValueNobodyWroteIsCaught() {
        assertEquals(Verdict.NOT_LINEARIZABLE, verdict(write(1, 1, 0, 1), read(2, 7L, 2, 3)));
    }

    @Test
    void aNewThenOldReadInversionIsCaught() {
        // A slow write overlaps two reads. Either read may see it, but once one read has seen the new
        // value, a later read must not go back to the old one.
        assertEquals(Verdict.NOT_LINEARIZABLE, verdict(
                write(1, 1, 0, 1), write(1, 2, 10, 100), read(2, 2L, 20, 30), read(3, 1L, 40, 50)));
        assertEquals(Verdict.LINEARIZABLE, verdict(
                write(1, 1, 0, 1), write(1, 2, 10, 100), read(2, 1L, 20, 30), read(3, 2L, 40, 50)));
    }

    @Test
    void aWriteWithNoAnswerMayOrMayNotHaveHappened() {
        assertEquals(Verdict.LINEARIZABLE, verdict(write(1, 1, 0, 1), unknownWrite(2, 2, 5), read(3, 2L, 50, 60)));
        assertEquals(Verdict.LINEARIZABLE, verdict(write(1, 1, 0, 1), unknownWrite(2, 2, 5), read(3, 1L, 50, 60)));
        // But it can't take effect before it was even sent.
        assertEquals(Verdict.NOT_LINEARIZABLE, verdict(write(1, 1, 0, 1), read(3, 2L, 2, 3), unknownWrite(2, 2, 5)));
        // And once it has been seen, it can't be un-seen.
        assertEquals(Verdict.NOT_LINEARIZABLE, verdict(
                write(1, 1, 0, 1), unknownWrite(2, 2, 5), read(3, 2L, 50, 60), read(3, 1L, 70, 80)));
    }

    @Test
    void lostAndDuplicatedIncrementsAreCaught() {
        // Two concurrent INCRs from 0 must return 1 and 2, never 1 and 1 (a lost update).
        assertEquals(Verdict.LINEARIZABLE, verdict(incr(1, 1L, 0, 10), incr(2, 2L, 1, 9)));
        assertEquals(Verdict.NOT_LINEARIZABLE, verdict(incr(1, 1L, 0, 10), incr(2, 1L, 1, 9)));
        // One INCR applied twice (say, a log entry replayed) shows up as a jump.
        assertEquals(Verdict.NOT_LINEARIZABLE, verdict(write(1, 5, 0, 1), incr(1, 6L, 2, 3), read(2, 7L, 4, 5)));
    }

    @Test
    void largeConcurrentHistoriesCheckQuickly() {
        List<Op> history = linearizableHistory(new Random(42), 20_000, 10);
        LinearizabilityChecker.Result ok = LinearizabilityChecker.check("k", history, Duration.ofSeconds(30));
        assertEquals(Verdict.LINEARIZABLE, ok.verdict(), ok.explanation());
        assertTrue(ok.millis() < 10_000, "took " + ok.millis() + " ms");
    }

    @Test
    void oneBadReadInALongHistoryIsFound() {
        // Proving a history wrong means ruling out every possible order before the bad operation,
        // which grows with how many operations overlap, so this uses 4 concurrent clients.
        List<Op> history = linearizableHistory(new Random(7), 5_000, 4);
        assertEquals(Verdict.LINEARIZABLE, LinearizabilityChecker.check("k", history, Duration.ofSeconds(30)).verdict());

        // Corrupt a single read in the middle to a value that was never written.
        List<Op> bad = new ArrayList<>(history);
        int i = bad.size() / 2;
        while (bad.get(i).type() != Type.READ || bad.get(i).outcome() != Outcome.OK) i++;
        Op r = bad.get(i);
        bad.set(i, new Op(r.client(), r.key(), r.type(), 999_999_999L, r.invokedAt(), r.completedAt(), r.outcome()));
        LinearizabilityChecker.Result caught = LinearizabilityChecker.check("k", bad, Duration.ofSeconds(30));
        assertEquals(Verdict.NOT_LINEARIZABLE, caught.verdict());
        assertTrue(caught.explanation().contains("999999999"), caught.explanation());
    }

    /**
     * Simulates a correct register: every operation takes effect at a random instant between its
     * call and return, so the history is linearizable by construction. Some writes "time out".
     */
    static List<Op> linearizableHistory(Random rnd, int count, int clients) {
        record Pending(int client, Type type, long value, long call, long ret, long at, boolean unknown) {}
        long[] clientFree = new long[clients];
        List<Pending> pending = new ArrayList<>();
        for (int n = 0; n < count; n++) {
            int c = rnd.nextInt(clients);
            long call = clientFree[c] + rnd.nextInt(50);
            long ret = call + 1 + rnd.nextInt(200);
            clientFree[c] = ret + 1;
            Type type = Type.values()[rnd.nextInt(3)];
            boolean unknown = type != Type.READ && rnd.nextInt(200) == 0;
            pending.add(new Pending(c, type, (long) n * 1000, call, ret, call + rnd.nextLong(ret - call + 1), unknown));
        }
        pending.sort(Comparator.comparingLong(Pending::at));
        long state = Long.MIN_VALUE; // absent
        List<Op> ops = new ArrayList<>();
        for (Pending p : pending) {
            Long seen = null;
            switch (p.type) {
                case READ -> seen = state == Long.MIN_VALUE ? null : state;
                case WRITE -> {
                    if (p.unknown && rnd.nextBoolean()) break; // a timed-out write that never applied
                    state = p.value;
                    seen = p.value;
                }
                case INCR -> {
                    state = (state == Long.MIN_VALUE ? 0 : state) + 1;
                    seen = state;
                }
            }
            Long recorded = p.type == Type.WRITE ? (Long) p.value : seen;
            if (p.unknown && p.type == Type.INCR) recorded = null;
            ops.add(new Op(p.client, "k", p.type, recorded, p.call, p.ret, p.unknown ? Outcome.UNKNOWN : Outcome.OK));
        }
        ops.sort(Comparator.comparingLong(Op::invokedAt));
        return ops;
    }
}
