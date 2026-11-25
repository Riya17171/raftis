package com.raftis.chaos;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Decides whether a history of concurrent operations is <b>linearizable</b>: whether there is a
 * single order of all the operations that (1) respects real time, so an operation that finished
 * before another started comes first, and (2) gives every operation the result its client actually
 * saw, when replayed against one simple in-memory register. If no such order exists, the database
 * returned something no correct single copy of the data could have returned.
 *
 * <p>The search is the Wing &amp; Gong algorithm with Lowe's memoization, the same algorithm the
 * Knossos (Jepsen) and Porcupine checkers use: walk the history in time order, try to "linearize"
 * each operation that has started, backtrack when an operation's completion is reached before it
 * could be placed, and remember every (set of placed operations, register value) already explored
 * so no state is searched twice.
 *
 * <p>Two things keep it fast:
 * <ul>
 *   <li><b>Per-key checking.</b> Linearizability is compositional: a history is linearizable if and
 *       only if each key's sub-history is. So every key is checked separately (and in parallel).</li>
 *   <li><b>Unknown outcomes.</b> An operation that never got an answer may have taken effect at any
 *       moment after it started, or never. It is treated as completing at the end of time, so the
 *       search can place it anywhere after its start, including after everything else, which is the
 *       same as it never happening. Reads with no answer change nothing, so they are dropped.</li>
 * </ul>
 */
final class LinearizabilityChecker {
    enum Verdict { LINEARIZABLE, NOT_LINEARIZABLE, TIMED_OUT }

    record Result(String key, Verdict verdict, int operations, String explanation, long millis) {}

    private static final long ABSENT = Long.MIN_VALUE;      // the key doesn't exist
    private static final long INVALID = Long.MIN_VALUE + 1; // this operation can't come next

    private LinearizabilityChecker() {}

    /** Checks every key separately, in parallel. Results are in key order. */
    static List<Result> checkAll(List<Op> history, Duration budgetPerKey) {
        Map<String, List<Op>> byKey = new LinkedHashMap<>();
        history.stream().sorted(Comparator.comparing(Op::key))
                .forEach(op -> byKey.computeIfAbsent(op.key(), k -> new ArrayList<>()).add(op));
        return byKey.entrySet().parallelStream()
                .map(e -> check(e.getKey(), e.getValue(), budgetPerKey))
                .toList();
    }

    /** The register model: the value after applying {@code op} to {@code state}, or INVALID. */
    private static long step(long state, Op op) {
        return switch (op.type()) {
            case READ -> (op.value() == null ? ABSENT : op.value()) == state ? state : INVALID;
            case WRITE -> op.value();
            case INCR -> {
                long next = (state == ABSENT ? 0 : state) + 1;
                yield op.outcome() == Op.Outcome.UNKNOWN || op.value() == next ? next : INVALID;
            }
        };
    }

    /** One entry in the time-ordered list: an operation's call or its return. */
    private static final class Entry {
        final int op;
        final boolean isCall;
        final long time;
        Entry match; // a call's return
        Entry prev;
        Entry next;

        Entry(int op, boolean isCall, long time) {
            this.op = op;
            this.isCall = isCall;
            this.time = time;
        }
    }

    private record Placed(Entry call, long stateBefore) {}

    /**
     * A memoized search state: which operations are placed, and the register's value. Operations
     * are numbered by start time, so the placed set is nearly always "all of 0..n, minus a few
     * holes" (operations still in flight, or with unknown outcomes). Storing just n and the holes
     * keeps each state a few bytes, however long the history is.
     */
    private static final class Seen {
        private final int[] shape; // [first unplaced, last placed, holes between them...]
        private final long state;
        private final int hash;

        Seen(BitSet placed, long state) {
            int first = placed.nextClearBit(0);
            int last = placed.length() - 1;
            int holes = 0;
            for (int i = placed.nextClearBit(first + 1); i < last; i = placed.nextClearBit(i + 1)) holes++;
            shape = new int[2 + holes];
            shape[0] = first;
            shape[1] = last;
            int k = 2;
            for (int i = placed.nextClearBit(first + 1); i < last; i = placed.nextClearBit(i + 1)) shape[k++] = i;
            this.state = state;
            this.hash = 31 * Arrays.hashCode(shape) + Long.hashCode(state);
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof Seen other && other.hash == hash && other.state == state && Arrays.equals(other.shape, shape);
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    static Result check(String key, List<Op> history, Duration budget) {
        long started = System.nanoTime();
        long deadline = started + budget.toNanos();
        List<Op> ops = history.stream()
                .filter(op -> !(op.type() == Op.Type.READ && op.outcome() == Op.Outcome.UNKNOWN))
                .filter(op -> !unobservedUnknownWrite(op, history))
                .sorted(Comparator.comparingLong(Op::invokedAt))
                .toList();

        // Build the list of calls and returns in time order. On a tie, calls go first, which treats
        // the two operations as overlapping: the more permissive reading, so no false alarms.
        List<Entry> entries = new ArrayList<>(ops.size() * 2);
        for (int i = 0; i < ops.size(); i++) {
            Op op = ops.get(i);
            Entry call = new Entry(i, true, op.invokedAt());
            Entry ret = new Entry(i, false, op.outcome() == Op.Outcome.UNKNOWN ? Long.MAX_VALUE : op.completedAt());
            call.match = ret;
            entries.add(call);
            entries.add(ret);
        }
        entries.sort(Comparator.comparingLong((Entry e) -> e.time).thenComparing(e -> !e.isCall));
        Entry head = new Entry(-1, false, Long.MIN_VALUE);
        Entry last = head;
        for (Entry e : entries) {
            last.next = e;
            e.prev = last;
            last = e;
        }

        ArrayDeque<Placed> stack = new ArrayDeque<>();
        Set<Seen> seen = new HashSet<>();
        BitSet placed = new BitSet(ops.size());
        long state = ABSENT;
        int deepest = -1;
        int stuckOp = -1;
        long stateAtDeepest = ABSENT;
        long steps = 0;

        Entry entry = head.next;
        while (head.next != null) {
            if ((++steps & 1023) == 0 && System.nanoTime() > deadline) {
                return new Result(key, Verdict.TIMED_OUT, ops.size(),
                        "gave up after " + budget.toSeconds() + " s (" + stack.size() + " of " + ops.size() + " placed)",
                        (System.nanoTime() - started) / 1_000_000);
            }
            if (entry.isCall) {
                long next = step(state, ops.get(entry.op));
                if (next != INVALID) {
                    placed.set(entry.op);
                    if (seen.add(new Seen(placed, next))) {
                        // Place this operation here and restart from the earliest remaining entry.
                        stack.push(new Placed(entry, state));
                        state = next;
                        lift(entry);
                        entry = head.next;
                        continue;
                    }
                    placed.clear(entry.op); // this exact situation was already explored
                }
                entry = entry.next; // try a different operation in this position
            } else {
                // We reached an operation's return without having placed it: some earlier choice was wrong.
                if (stack.size() > deepest) {
                    deepest = stack.size();
                    stuckOp = entry.op;
                    stateAtDeepest = state;
                }
                if (stack.isEmpty()) {
                    return new Result(key, Verdict.NOT_LINEARIZABLE, ops.size(),
                            explain(key, ops, deepest, stuckOp, stateAtDeepest),
                            (System.nanoTime() - started) / 1_000_000);
                }
                Placed undo = stack.pop();
                placed.clear(undo.call.op);
                state = undo.stateBefore;
                unlift(undo.call);
                entry = undo.call.next;
            }
        }
        return new Result(key, Verdict.LINEARIZABLE, ops.size(), "", (System.nanoTime() - started) / 1_000_000);
    }

    /**
     * Operations that can be left out without changing the answer:
     * <ul>
     *   <li>a read with no answer: it tells us nothing and changes nothing;</li>
     *   <li>a write with no answer whose value nobody ever saw. "It never happened" is then
     *       always a valid explanation for it. To be safe this also looks for values that INCRs
     *       could have derived from it (value + 1, + 2, ...).</li>
     * </ul>
     * Without this, every timed-out write during a partition stays "maybe pending" until the end
     * of the history, and each one doubles the number of orders the search must rule out.
     */
    private static boolean unobservedUnknownWrite(Op op, List<Op> history) {
        if (op.type() != Op.Type.WRITE || op.outcome() != Op.Outcome.UNKNOWN) return false;
        long incrs = history.stream().filter(o -> o.type() == Op.Type.INCR).count();
        long low = op.value();
        long high = low + incrs;
        for (Op o : history) {
            boolean answered = o.outcome() == Op.Outcome.OK && o.type() != Op.Type.WRITE && o.value() != null;
            if (answered && o.value() >= low && o.value() <= high) return false;
        }
        return true;
    }

    /** Removes a call and its return from the list (dancing links: they can be put back exactly). */
    private static void lift(Entry call) {
        call.prev.next = call.next;
        if (call.next != null) call.next.prev = call.prev;
        Entry ret = call.match;
        ret.prev.next = ret.next;
        if (ret.next != null) ret.next.prev = ret.prev;
    }

    private static void unlift(Entry call) {
        Entry ret = call.match;
        ret.prev.next = ret;
        if (ret.next != null) ret.next.prev = ret;
        call.prev.next = call;
        if (call.next != null) call.next.prev = call;
    }

    private static String explain(String key, List<Op> ops, int deepest, int stuckOp, long state) {
        Op stuck = ops.get(stuckOp);
        StringBuilder sb = new StringBuilder();
        sb.append("no valid order exists for key ").append(key).append(" (").append(ops.size()).append(" operations)\n");
        sb.append("  at most ").append(deepest).append(" operations can be put in a valid order; after them ")
                .append(key).append(" = ").append(state == ABSENT ? "(nil)" : state).append('\n');
        sb.append("  and then this one can never be placed:\n    ").append(stuck).append('\n');
        sb.append("  operations overlapping it, or just before it:\n");
        long from = stuck.invokedAt();
        List<Op> context = new ArrayList<>();
        for (Op op : ops) {
            long end = op.outcome() == Op.Outcome.UNKNOWN ? Long.MAX_VALUE : op.completedAt();
            boolean overlaps = op.invokedAt() <= stuck.completedAt() && end >= from;
            if (op != stuck && overlaps) context.add(op);
        }
        // The last few writes that finished before it started: the values it should have seen.
        ops.stream()
                .filter(op -> op.type() != Op.Type.READ && op.outcome() == Op.Outcome.OK && op.completedAt() < from)
                .sorted(Comparator.comparingLong(Op::completedAt).reversed())
                .limit(3)
                .forEach(context::add);
        context.stream().distinct().sorted(Comparator.comparingLong(Op::invokedAt)).limit(15)
                .forEach(op -> sb.append("    ").append(op).append('\n'));
        return sb.toString();
    }
}
