package com.raftis.command;

import com.raftis.store.ByteKey;
import com.raftis.store.ZSet;

import java.util.List;

import static com.raftis.command.CommandSpec.WRITE;

/** Sorted-set commands: ZADD, ZRANK, ZRANGE, ZRANGEBYSCORE, ZINCRBY... */
final class ZSetCommands {
    private ZSetCommands() {}

    static void register(CommandTable t) {
        t.add("zadd", -4, WRITE, ZSetCommands::zadd);
        t.add("zrem", -3, WRITE, ZSetCommands::zrem);
        t.add("zscore", 3, 0, ZSetCommands::zscore);
        t.add("zincrby", 4, WRITE, ZSetCommands::zincrby);
        t.add("zcard", 2, 0, ZSetCommands::zcard);
        t.add("zrank", 3, 0, ZSetCommands::zrank);
        t.add("zrange", -4, 0, ZSetCommands::zrange);
        t.add("zrangebyscore", -4, 0, ZSetCommands::zrangebyscore);
    }

    private static ZSet getZSet(Context ctx, ByteKey key, boolean create) {
        Object value = ctx.db().get(key);
        if (value == null) {
            if (!create) return null;
            ZSet zset = new ZSet();
            ctx.db().put(key, zset);
            return zset;
        }
        if (value instanceof ZSet zset) return zset;
        throw CommandException.wrongType();
    }

    /** ZADD key [NX|XX] [CH] score member [score member ...] */
    private static void zadd(Context ctx, List<byte[]> a) {
        boolean nx = false, xx = false, ch = false;
        int i = 2;
        for (; i < a.size(); i++) {
            if (Args.is(a.get(i), "NX")) nx = true;
            else if (Args.is(a.get(i), "XX")) xx = true;
            else if (Args.is(a.get(i), "CH")) ch = true;
            else break;
        }
        int remaining = a.size() - i;
        if (remaining == 0 || remaining % 2 != 0) throw CommandException.syntax();
        if (nx && xx) throw new CommandException("ERR XX and NX options at the same time are not compatible");
        // Validate every score before changing anything, so a bad score doesn't leave a half-done ZADD.
        double[] scores = new double[remaining / 2];
        for (int j = 0; j < scores.length; j++) scores[j] = Args.toDouble(a.get(i + 2 * j));

        ByteKey key = Args.key(a, 1);
        ZSet zset = getZSet(ctx, key, !xx);
        if (zset == null) {
            ctx.out.integer(0);
            return;
        }
        long added = 0, changed = 0;
        for (int j = 0; j < scores.length; j++) {
            byte[] member = a.get(i + 2 * j + 1);
            Double old = zset.score(member);
            if (old == null) {
                if (xx) continue;
                zset.add(member, scores[j]);
                added++;
            } else if (!nx && old != scores[j]) {
                zset.add(member, scores[j]);
                changed++;
            }
        }
        if (zset.size() == 0) ctx.db().delete(key);
        if (added + changed > 0) ctx.dirty();
        ctx.out.integer(ch ? added + changed : added);
    }

    private static void zrem(Context ctx, List<byte[]> a) {
        ByteKey key = Args.key(a, 1);
        ZSet zset = getZSet(ctx, key, false);
        long removed = 0;
        if (zset != null) {
            for (int i = 2; i < a.size(); i++) {
                if (zset.remove(a.get(i))) removed++;
            }
            if (zset.size() == 0) ctx.db().delete(key);
        }
        if (removed > 0) ctx.dirty();
        ctx.out.integer(removed);
    }

    private static void zscore(Context ctx, List<byte[]> a) {
        ZSet zset = getZSet(ctx, Args.key(a, 1), false);
        Double score = zset == null ? null : zset.score(a.get(2));
        ctx.out.bulk(score == null ? null : Args.formatDouble(score));
    }

    private static void zincrby(Context ctx, List<byte[]> a) {
        double delta = Args.toDouble(a.get(2));
        ByteKey key = Args.key(a, 1);
        byte[] member = a.get(3);
        ZSet existing = getZSet(ctx, key, false);
        Double old = existing == null ? null : existing.score(member);
        double result = (old == null ? 0 : old) + delta;
        if (Double.isNaN(result)) throw new CommandException("ERR resulting score is not a number (NaN)");
        ZSet zset = existing != null ? existing : getZSet(ctx, key, true);
        zset.add(member, result);
        ctx.dirty();
        ctx.out.bulk(Args.formatDouble(result));
    }

    private static void zcard(Context ctx, List<byte[]> a) {
        ZSet zset = getZSet(ctx, Args.key(a, 1), false);
        ctx.out.integer(zset == null ? 0 : zset.size());
    }

    private static void zrank(Context ctx, List<byte[]> a) {
        ZSet zset = getZSet(ctx, Args.key(a, 1), false);
        long rank = zset == null ? -1 : zset.rank(a.get(2));
        if (rank < 0) ctx.out.nullBulk();
        else ctx.out.integer(rank);
    }

    /** ZRANGE key start stop [WITHSCORES] */
    private static void zrange(Context ctx, List<byte[]> a) {
        long start = Args.toLong(a.get(2));
        long stop = Args.toLong(a.get(3));
        boolean withScores = false;
        if (a.size() == 5 && Args.is(a.get(4), "WITHSCORES")) withScores = true;
        else if (a.size() > 4) throw CommandException.syntax();

        ZSet zset = getZSet(ctx, Args.key(a, 1), false);
        if (zset == null) {
            ctx.out.arrayHeader(0);
            return;
        }
        int n = zset.size();
        if (start < 0) start += n;
        if (stop < 0) stop += n;
        if (start < 0) start = 0;
        if (start > stop || start >= n) {
            ctx.out.arrayHeader(0);
            return;
        }
        if (stop >= n) stop = n - 1;
        writeNodes(ctx, zset.rangeByRank(start, stop), withScores);
    }

    /** ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]. "(" makes a bound exclusive. */
    private static void zrangebyscore(Context ctx, List<byte[]> a) {
        Bound min = Bound.parse(a.get(2));
        Bound max = Bound.parse(a.get(3));
        boolean withScores = false;
        long offset = 0, limit = -1;
        for (int i = 4; i < a.size(); i++) {
            if (Args.is(a.get(i), "WITHSCORES")) {
                withScores = true;
            } else if (Args.is(a.get(i), "LIMIT") && i + 2 < a.size()) {
                offset = Args.toLong(a.get(i + 1));
                limit = Args.toLong(a.get(i + 2));
                i += 2;
            } else {
                throw CommandException.syntax();
            }
        }
        ZSet zset = getZSet(ctx, Args.key(a, 1), false);
        if (zset == null || offset < 0) {
            ctx.out.arrayHeader(0);
            return;
        }
        writeNodes(ctx, zset.rangeByScore(min.value, min.exclusive, max.value, max.exclusive, offset, limit), withScores);
    }

    private static void writeNodes(Context ctx, List<ZSet.Node> nodes, boolean withScores) {
        ctx.out.arrayHeader(withScores ? nodes.size() * 2 : nodes.size());
        for (ZSet.Node node : nodes) {
            ctx.out.bulk(node.member());
            if (withScores) ctx.out.bulk(Args.formatDouble(node.score()));
        }
    }

    private record Bound(double value, boolean exclusive) {
        static Bound parse(byte[] raw) {
            boolean exclusive = raw.length > 0 && raw[0] == '(';
            byte[] number = exclusive ? java.util.Arrays.copyOfRange(raw, 1, raw.length) : raw;
            try {
                return new Bound(Args.toDouble(number), exclusive);
            } catch (CommandException e) {
                throw new CommandException("ERR min or max is not a float");
            }
        }
    }
}
