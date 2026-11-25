package com.raftis.command;

import com.raftis.store.ByteKey;
import com.raftis.store.RHash;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.raftis.command.CommandSpec.WRITE;

/** Hash commands: HSET, HGET, HDEL, HGETALL, HINCRBY... */
final class HashCommands {
    private HashCommands() {}

    static void register(CommandTable t) {
        t.add("hset", -4, WRITE, (ctx, a) -> hset(ctx, a, false));
        t.add("hmset", -4, WRITE, (ctx, a) -> hset(ctx, a, true));
        t.add("hget", 3, 0, HashCommands::hget);
        t.add("hmget", -3, 0, HashCommands::hmget);
        t.add("hdel", -3, WRITE, HashCommands::hdel);
        t.add("hgetall", 2, 0, HashCommands::hgetall);
        t.add("hkeys", 2, 0, (ctx, a) -> keysOrValues(ctx, a, true));
        t.add("hvals", 2, 0, (ctx, a) -> keysOrValues(ctx, a, false));
        t.add("hlen", 2, 0, HashCommands::hlen);
        t.add("hexists", 3, 0, HashCommands::hexists);
        t.add("hincrby", 4, WRITE, HashCommands::hincrby);
    }

    private static RHash getHash(Context ctx, ByteKey key, boolean create) {
        Object value = ctx.db().get(key);
        if (value == null) {
            if (!create) return null;
            RHash hash = new RHash();
            ctx.db().put(key, hash);
            return hash;
        }
        if (value instanceof RHash hash) return hash;
        throw CommandException.wrongType();
    }

    private static void hset(Context ctx, List<byte[]> a, boolean legacyReply) {
        if ((a.size() - 2) % 2 != 0) throw CommandException.wrongArgs(Args.lower(a.get(0)));
        RHash hash = getHash(ctx, Args.key(a, 1), true);
        long added = 0;
        for (int i = 2; i < a.size(); i += 2) {
            if (hash.put(new ByteKey(a.get(i)), a.get(i + 1)) == null) added++;
        }
        ctx.dirty();
        if (legacyReply) ctx.out.simple("OK");
        else ctx.out.integer(added);
    }

    private static void hget(Context ctx, List<byte[]> a) {
        RHash hash = getHash(ctx, Args.key(a, 1), false);
        ctx.out.bulk(hash == null ? null : hash.get(new ByteKey(a.get(2))));
    }

    private static void hmget(Context ctx, List<byte[]> a) {
        RHash hash = getHash(ctx, Args.key(a, 1), false);
        List<byte[]> values = new ArrayList<>(a.size() - 2);
        for (int i = 2; i < a.size(); i++) values.add(hash == null ? null : hash.get(new ByteKey(a.get(i))));
        ctx.out.bulkArray(values);
    }

    private static void hdel(Context ctx, List<byte[]> a) {
        ByteKey key = Args.key(a, 1);
        RHash hash = getHash(ctx, key, false);
        long removed = 0;
        if (hash != null) {
            for (int i = 2; i < a.size(); i++) {
                if (hash.remove(new ByteKey(a.get(i))) != null) removed++;
            }
            if (hash.isEmpty()) ctx.db().delete(key);
        }
        if (removed > 0) ctx.dirty();
        ctx.out.integer(removed);
    }

    private static void hgetall(Context ctx, List<byte[]> a) {
        RHash hash = getHash(ctx, Args.key(a, 1), false);
        if (hash == null) {
            ctx.out.arrayHeader(0);
            return;
        }
        ctx.out.arrayHeader(hash.size() * 2);
        for (Map.Entry<ByteKey, byte[]> e : hash.entrySet()) {
            ctx.out.bulk(e.getKey().bytes()).bulk(e.getValue());
        }
    }

    private static void keysOrValues(Context ctx, List<byte[]> a, boolean keys) {
        RHash hash = getHash(ctx, Args.key(a, 1), false);
        if (hash == null) {
            ctx.out.arrayHeader(0);
            return;
        }
        ctx.out.arrayHeader(hash.size());
        for (Map.Entry<ByteKey, byte[]> e : hash.entrySet()) ctx.out.bulk(keys ? e.getKey().bytes() : e.getValue());
    }

    private static void hlen(Context ctx, List<byte[]> a) {
        RHash hash = getHash(ctx, Args.key(a, 1), false);
        ctx.out.integer(hash == null ? 0 : hash.size());
    }

    private static void hexists(Context ctx, List<byte[]> a) {
        RHash hash = getHash(ctx, Args.key(a, 1), false);
        ctx.out.integer(hash != null && hash.containsKey(new ByteKey(a.get(2))) ? 1 : 0);
    }

    private static void hincrby(Context ctx, List<byte[]> a) {
        long delta = Args.toLong(a.get(3));
        RHash hash = getHash(ctx, Args.key(a, 1), true);
        ByteKey field = new ByteKey(a.get(2));
        byte[] current = hash.get(field);
        long value = 0;
        if (current != null) {
            Long parsed = Args.tryLong(current);
            if (parsed == null) throw new CommandException("ERR hash value is not an integer");
            value = parsed;
        }
        long result;
        try {
            result = Math.addExact(value, delta);
        } catch (ArithmeticException e) {
            throw CommandException.overflow();
        }
        hash.put(field, Args.bytes(result));
        ctx.dirty();
        ctx.out.integer(result);
    }
}
