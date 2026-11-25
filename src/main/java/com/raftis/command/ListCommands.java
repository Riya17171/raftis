package com.raftis.command;

import com.raftis.store.ByteKey;
import com.raftis.store.RList;

import java.util.List;

import static com.raftis.command.CommandSpec.WRITE;

/** List commands: LPUSH, RPUSH, LPOP, RPOP, LLEN, LRANGE, LINDEX. */
final class ListCommands {
    private ListCommands() {}

    static void register(CommandTable t) {
        t.add("lpush", -3, WRITE, (ctx, a) -> push(ctx, a, true));
        t.add("rpush", -3, WRITE, (ctx, a) -> push(ctx, a, false));
        t.add("lpop", -2, WRITE, (ctx, a) -> pop(ctx, a, true));
        t.add("rpop", -2, WRITE, (ctx, a) -> pop(ctx, a, false));
        t.add("llen", 2, 0, ListCommands::llen);
        t.add("lrange", 4, 0, ListCommands::lrange);
        t.add("lindex", 3, 0, ListCommands::lindex);
    }

    private static RList getList(Context ctx, ByteKey key, boolean create) {
        Object value = ctx.db().get(key);
        if (value == null) {
            if (!create) return null;
            RList list = new RList();
            ctx.db().put(key, list);
            return list;
        }
        if (value instanceof RList list) return list;
        throw CommandException.wrongType();
    }

    private static void push(Context ctx, List<byte[]> a, boolean left) {
        RList list = getList(ctx, Args.key(a, 1), true);
        for (int i = 2; i < a.size(); i++) {
            if (left) list.addFirst(a.get(i));
            else list.addLast(a.get(i));
        }
        ctx.dirty();
        ctx.out.integer(list.size());
    }

    /** LPOP key [count]. Empty lists are deleted, as in Redis. */
    private static void pop(Context ctx, List<byte[]> a, boolean left) {
        if (a.size() > 3) throw CommandException.wrongArgs(Args.lower(a.get(0)));
        long count = -1;
        if (a.size() == 3) {
            count = Args.toLong(a.get(2));
            if (count < 0) throw new CommandException("ERR value is out of range, must be positive");
        }
        ByteKey key = Args.key(a, 1);
        RList list = getList(ctx, key, false);
        if (list == null) {
            if (count < 0) ctx.out.nullBulk();
            else ctx.out.nullArray();
            return;
        }
        if (count < 0) {
            ctx.out.bulk(left ? list.pollFirst() : list.pollLast());
        } else {
            int n = (int) Math.min(count, list.size());
            ctx.out.arrayHeader(n);
            for (int i = 0; i < n; i++) ctx.out.bulk(left ? list.pollFirst() : list.pollLast());
        }
        if (list.size() == 0) ctx.db().delete(key);
        ctx.dirty();
    }

    private static void llen(Context ctx, List<byte[]> a) {
        RList list = getList(ctx, Args.key(a, 1), false);
        ctx.out.integer(list == null ? 0 : list.size());
    }

    private static void lrange(Context ctx, List<byte[]> a) {
        long start = Args.toLong(a.get(2));
        long stop = Args.toLong(a.get(3));
        RList list = getList(ctx, Args.key(a, 1), false);
        if (list == null) {
            ctx.out.arrayHeader(0);
            return;
        }
        int n = list.size();
        // Negative indexes count from the end: -1 is the last element.
        if (start < 0) start += n;
        if (stop < 0) stop += n;
        if (start < 0) start = 0;
        if (start > stop || start >= n) {
            ctx.out.arrayHeader(0);
            return;
        }
        if (stop >= n) stop = n - 1;
        ctx.out.arrayHeader((int) (stop - start + 1));
        for (long i = start; i <= stop; i++) ctx.out.bulk(list.get((int) i));
    }

    private static void lindex(Context ctx, List<byte[]> a) {
        long index = Args.toLong(a.get(2));
        RList list = getList(ctx, Args.key(a, 1), false);
        if (list == null) {
            ctx.out.nullBulk();
            return;
        }
        if (index < 0) index += list.size();
        if (index < 0 || index >= list.size()) ctx.out.nullBulk();
        else ctx.out.bulk(list.get((int) index));
    }
}
