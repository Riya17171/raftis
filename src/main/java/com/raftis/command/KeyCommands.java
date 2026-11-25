package com.raftis.command;

import com.raftis.store.ByteKey;
import com.raftis.store.Glob;
import com.raftis.store.Keyspace;
import com.raftis.store.RHash;
import com.raftis.store.RList;
import com.raftis.store.ZSet;

import java.util.ArrayList;
import java.util.List;

import static com.raftis.command.CommandSpec.WRITE;

/** Commands that work on any key: DEL, EXISTS, TYPE, KEYS, EXPIRE, TTL... */
final class KeyCommands {
    private KeyCommands() {}

    static void register(CommandTable t) {
        t.add("del", -2, WRITE, KeyCommands::del);
        t.add("unlink", -2, WRITE, KeyCommands::del);
        t.add("exists", -2, 0, KeyCommands::exists);
        t.add("type", 2, 0, (ctx, a) -> ctx.out.simple(typeName(ctx.db().get(Args.key(a, 1)))));
        t.add("keys", 2, 0, KeyCommands::keys);
        t.add("expire", 3, WRITE, (ctx, a) -> expire(ctx, a, 1000, false));
        t.add("pexpire", 3, WRITE, (ctx, a) -> expire(ctx, a, 1, false));
        t.add("expireat", 3, WRITE, (ctx, a) -> expire(ctx, a, 1000, true));
        t.add("pexpireat", 3, WRITE, (ctx, a) -> expire(ctx, a, 1, true));
        t.add("ttl", 2, 0, (ctx, a) -> ttl(ctx, a, false));
        t.add("pttl", 2, 0, (ctx, a) -> ttl(ctx, a, true));
        t.add("persist", 2, WRITE, KeyCommands::persist);
    }

    static String typeName(Object value) {
        return switch (value) {
            case null -> "none";
            case byte[] b -> "string";
            case RList l -> "list";
            case RHash h -> "hash";
            case ZSet z -> "zset";
            default -> throw new IllegalStateException("unknown type " + value.getClass());
        };
    }

    private static void del(Context ctx, List<byte[]> a) {
        long deleted = 0;
        for (int i = 1; i < a.size(); i++) {
            if (ctx.db().delete(Args.key(a, i))) deleted++;
        }
        if (deleted > 0) ctx.dirty();
        ctx.out.integer(deleted);
    }

    private static void exists(Context ctx, List<byte[]> a) {
        long count = 0;
        for (int i = 1; i < a.size(); i++) {
            if (ctx.db().get(Args.key(a, i)) != null) count++;
        }
        ctx.out.integer(count);
    }

    private static void keys(Context ctx, List<byte[]> a) {
        byte[] pattern = a.get(1);
        Keyspace db = ctx.db();
        List<byte[]> out = new ArrayList<>();
        for (ByteKey k : db.keys()) {
            if (!db.isExpired(k) && Glob.match(pattern, k.bytes())) out.add(k.bytes());
        }
        ctx.out.bulkArray(out);
    }

    /**
     * EXPIRE, PEXPIRE, EXPIREAT and PEXPIREAT all become PEXPIREAT with an absolute time in the
     * log, so replaying it later doesn't give keys a fresh TTL.
     */
    private static void expire(Context ctx, List<byte[]> a, long unitMs, boolean absolute) {
        ByteKey key = Args.key(a, 1);
        long value = Args.toLong(a.get(2));
        long at;
        try {
            long ms = Math.multiplyExact(value, unitMs);
            at = absolute ? ms : Math.addExact(ctx.db().now(), ms);
        } catch (ArithmeticException e) {
            throw new CommandException("ERR invalid expire time in '" + Args.lower(a.get(0)) + "' command");
        }
        Keyspace db = ctx.db();
        if (db.get(key) == null) {
            ctx.out.integer(0);
            return;
        }
        if (db.alreadyExpired(at)) {
            db.delete(key);
            ctx.propagateAs(Args.cmd("DEL", key));
        } else {
            db.setExpireAt(key, at);
            ctx.propagateAs(Args.cmd("PEXPIREAT", key, at));
        }
        ctx.out.integer(1);
    }

    private static void ttl(Context ctx, List<byte[]> a, boolean millis) {
        ByteKey key = Args.key(a, 1);
        Keyspace db = ctx.db();
        if (db.get(key) == null) {
            ctx.out.integer(-2);
            return;
        }
        long at = db.expireAt(key);
        if (at < 0) {
            ctx.out.integer(-1);
            return;
        }
        long remaining = Math.max(0, at - db.now());
        ctx.out.integer(millis ? remaining : (remaining + 500) / 1000);
    }

    private static void persist(Context ctx, List<byte[]> a) {
        ByteKey key = Args.key(a, 1);
        if (ctx.db().get(key) != null && ctx.db().persist(key)) {
            ctx.dirty();
            ctx.out.integer(1);
        } else {
            ctx.out.integer(0);
        }
    }
}
