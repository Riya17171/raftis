package com.raftis.command;

import com.raftis.store.ByteKey;
import com.raftis.store.Keyspace;

import java.util.ArrayList;
import java.util.List;

import static com.raftis.command.CommandSpec.WRITE;

/** String commands: GET, SET, INCR, APPEND, MGET, MSET... */
final class StringCommands {
    private StringCommands() {}

    static void register(CommandTable t) {
        t.add("get", 2, 0, (ctx, a) -> ctx.out.bulk(getString(ctx, Args.key(a, 1))));
        t.add("set", -3, WRITE, StringCommands::set);
        t.add("setex", 4, WRITE, StringCommands::setex);
        t.add("setnx", 3, WRITE, StringCommands::setnx);
        t.add("mget", -2, 0, StringCommands::mget);
        t.add("mset", -3, WRITE, StringCommands::mset);
        t.add("incr", 2, WRITE, (ctx, a) -> incrBy(ctx, Args.key(a, 1), 1));
        t.add("decr", 2, WRITE, (ctx, a) -> incrBy(ctx, Args.key(a, 1), -1));
        t.add("incrby", 3, WRITE, (ctx, a) -> incrBy(ctx, Args.key(a, 1), Args.toLong(a.get(2))));
        t.add("decrby", 3, WRITE, StringCommands::decrBy);
        t.add("append", 3, WRITE, StringCommands::append);
        t.add("strlen", 2, 0, StringCommands::strlen);
    }

    /** The string at {@code key}, null if missing, or a WRONGTYPE error if it holds another type. */
    static byte[] getString(Context ctx, ByteKey key) {
        Object value = ctx.db().get(key);
        if (value == null) return null;
        if (value instanceof byte[] b) return b;
        throw CommandException.wrongType();
    }

    /** SET key value [NX|XX] [GET] [EX s|PX ms|EXAT ts|PXAT ms-ts|KEEPTTL] */
    private static void set(Context ctx, List<byte[]> a) {
        ByteKey key = Args.key(a, 1);
        byte[] value = a.get(2);
        boolean nx = false, xx = false, keepTtl = false, get = false;
        long expireAt = -1;
        for (int i = 3; i < a.size(); i++) {
            String option = Args.upper(a.get(i));
            switch (option) {
                case "NX" -> nx = true;
                case "XX" -> xx = true;
                case "KEEPTTL" -> keepTtl = true;
                case "GET" -> get = true;
                case "EX", "PX", "EXAT", "PXAT" -> {
                    if (expireAt != -1 || i + 1 >= a.size()) throw CommandException.syntax();
                    expireAt = expireTime(ctx, option, a.get(++i));
                }
                default -> throw CommandException.syntax();
            }
        }
        if ((nx && xx) || (keepTtl && expireAt != -1)) throw CommandException.syntax();

        Keyspace db = ctx.db();
        Object old = db.get(key);
        if (get && old != null && !(old instanceof byte[])) throw CommandException.wrongType();
        boolean exists = old != null;
        if ((nx && exists) || (xx && !exists)) {
            if (get) ctx.out.bulk((byte[]) old);
            else ctx.out.nullBulk();
            return;
        }

        if (keepTtl) db.put(key, value);
        else db.set(key, value);

        if (expireAt != -1 && db.alreadyExpired(expireAt)) {
            db.delete(key);
            ctx.propagateAs(Args.cmd("DEL", key));
        } else if (expireAt != -1) {
            db.setExpireAt(key, expireAt);
            // Always log an absolute time, so a replay tomorrow doesn't extend the TTL.
            ctx.propagateAs(Args.cmd("SET", key, value, "PXAT", expireAt));
        } else {
            // NX/XX/GET already did their job; log the plain effect.
            ctx.propagateAs(keepTtl ? Args.cmd("SET", key, value, "KEEPTTL") : Args.cmd("SET", key, value));
        }
        if (get) ctx.out.bulk((byte[]) old);
        else ctx.out.simple("OK");
    }

    private static long expireTime(Context ctx, String option, byte[] raw) {
        long n = Args.toLong(raw);
        if (n <= 0) throw new CommandException("ERR invalid expire time in 'set' command");
        try {
            return switch (option) {
                case "EX" -> Math.addExact(ctx.db().now(), Math.multiplyExact(n, 1000));
                case "PX" -> Math.addExact(ctx.db().now(), n);
                case "EXAT" -> Math.multiplyExact(n, 1000);
                default -> n; // PXAT
            };
        } catch (ArithmeticException e) {
            throw new CommandException("ERR invalid expire time in 'set' command");
        }
    }

    private static void setex(Context ctx, List<byte[]> a) {
        ByteKey key = Args.key(a, 1);
        long seconds = Args.toLong(a.get(2));
        if (seconds <= 0) throw new CommandException("ERR invalid expire time in 'setex' command");
        long at;
        try {
            at = Math.addExact(ctx.db().now(), Math.multiplyExact(seconds, 1000));
        } catch (ArithmeticException e) {
            throw new CommandException("ERR invalid expire time in 'setex' command");
        }
        ctx.db().set(key, a.get(3));
        ctx.db().setExpireAt(key, at);
        ctx.propagateAs(Args.cmd("SET", key, a.get(3), "PXAT", at));
        ctx.out.simple("OK");
    }

    private static void setnx(Context ctx, List<byte[]> a) {
        ByteKey key = Args.key(a, 1);
        if (ctx.db().get(key) != null) {
            ctx.out.integer(0);
            return;
        }
        ctx.db().set(key, a.get(2));
        ctx.propagateAs(Args.cmd("SET", key, a.get(2)));
        ctx.out.integer(1);
    }

    private static void mget(Context ctx, List<byte[]> a) {
        List<byte[]> values = new ArrayList<>(a.size() - 1);
        for (int i = 1; i < a.size(); i++) {
            // MGET returns nil for keys holding other types instead of failing.
            values.add(ctx.db().get(Args.key(a, i)) instanceof byte[] b ? b : null);
        }
        ctx.out.bulkArray(values);
    }

    private static void mset(Context ctx, List<byte[]> a) {
        if ((a.size() - 1) % 2 != 0) throw CommandException.wrongArgs("mset");
        for (int i = 1; i < a.size(); i += 2) ctx.db().set(Args.key(a, i), a.get(i + 1));
        ctx.dirty();
        ctx.out.simple("OK");
    }

    private static void decrBy(Context ctx, List<byte[]> a) {
        long n = Args.toLong(a.get(2));
        if (n == Long.MIN_VALUE) throw new CommandException("ERR decrement would overflow");
        incrBy(ctx, Args.key(a, 1), -n);
    }

    private static void incrBy(Context ctx, ByteKey key, long delta) {
        byte[] current = getString(ctx, key);
        long value = 0;
        if (current != null) {
            Long parsed = Args.tryLong(current);
            if (parsed == null) throw CommandException.notInteger();
            value = parsed;
        }
        long result;
        try {
            result = Math.addExact(value, delta);
        } catch (ArithmeticException e) {
            throw CommandException.overflow();
        }
        ctx.db().put(key, Args.bytes(result)); // INCR keeps the key's TTL
        ctx.dirty();
        ctx.out.integer(result);
    }

    private static void append(Context ctx, List<byte[]> a) {
        ByteKey key = Args.key(a, 1);
        byte[] current = getString(ctx, key);
        byte[] suffix = a.get(2);
        byte[] result;
        if (current == null) {
            result = suffix;
        } else {
            result = new byte[current.length + suffix.length];
            System.arraycopy(current, 0, result, 0, current.length);
            System.arraycopy(suffix, 0, result, current.length, suffix.length);
        }
        ctx.db().put(key, result);
        ctx.dirty();
        ctx.out.integer(result.length);
    }

    private static void strlen(Context ctx, List<byte[]> a) {
        byte[] value = getString(ctx, Args.key(a, 1));
        ctx.out.integer(value == null ? 0 : value.length);
    }
}
