package com.raftis.command;

import com.raftis.store.ByteKey;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/** Helpers for reading command arguments and building replies. */
public final class Args {
    private Args() {}

    public static ByteKey key(List<byte[]> args, int index) {
        return new ByteKey(args.get(index));
    }

    /** Parses a 64-bit integer, or replies "ERR value is not an integer or out of range". */
    public static long toLong(byte[] b) {
        Long value = tryLong(b);
        if (value == null) throw CommandException.notInteger();
        return value;
    }

    /** Parses a 64-bit integer, or returns null. Rejects leading '+', spaces and empty input. */
    public static Long tryLong(byte[] b) {
        if (b.length == 0 || b.length > 20 || b[0] == '+') return null;
        try {
            return Long.parseLong(new String(b, StandardCharsets.ISO_8859_1));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /** Parses a score, accepting inf, +inf and -inf like Redis. NaN is rejected. */
    public static double toDouble(byte[] b) {
        String s = new String(b, StandardCharsets.ISO_8859_1);
        switch (s.toLowerCase(Locale.ROOT)) {
            case "inf", "+inf", "infinity", "+infinity" -> {
                return Double.POSITIVE_INFINITY;
            }
            case "-inf", "-infinity" -> {
                return Double.NEGATIVE_INFINITY;
            }
            default -> { }
        }
        // Java accepts things Redis doesn't ("1.5d", " 2", "0x1p3"); reject them.
        if (s.isEmpty() || !s.matches("[-+]?[0-9.]+([eE][-+]?[0-9]+)?")) throw CommandException.notFloat();
        try {
            double d = Double.parseDouble(s);
            if (Double.isNaN(d)) throw CommandException.notFloat();
            return d;
        } catch (NumberFormatException e) {
            throw CommandException.notFloat();
        }
    }

    /** Formats a score the way Redis replies with it: "1" not "1.0", "inf" for infinity. */
    public static String formatDouble(double d) {
        if (Double.isInfinite(d)) return d > 0 ? "inf" : "-inf";
        if (d == Math.rint(d) && Math.abs(d) < 1e17) return Long.toString((long) d);
        return Double.toString(d).replace('E', 'e');
    }

    public static byte[] bytes(long value) {
        return Long.toString(value).getBytes(StandardCharsets.ISO_8859_1);
    }

    public static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    public static String str(byte[] b) {
        return new String(b, StandardCharsets.UTF_8);
    }

    public static String upper(byte[] b) {
        return str(b).toUpperCase(Locale.ROOT);
    }

    public static String lower(byte[] b) {
        return str(b).toLowerCase(Locale.ROOT);
    }

    /** Case-insensitive comparison with an ASCII keyword. */
    public static boolean is(byte[] b, String keyword) {
        return b.length == keyword.length() && str(b).equalsIgnoreCase(keyword);
    }

    /** A short, single-line version of client input, safe to put in an error message. */
    public static String printable(byte[] b) {
        String s = str(b).replace('\r', ' ').replace('\n', ' ');
        return s.length() > 128 ? s.substring(0, 128) + "..." : s;
    }

    /** Builds a command for the append-only file from strings, keys, byte arrays and longs. */
    public static List<byte[]> cmd(Object... parts) {
        List<byte[]> out = new ArrayList<>(parts.length);
        for (Object part : parts) {
            out.add(switch (part) {
                case byte[] b -> b;
                case ByteKey k -> k.bytes();
                case String s -> bytes(s);
                case Long l -> bytes(l);
                default -> throw new IllegalArgumentException("can't encode " + part);
            });
        }
        return out;
    }
}
