package com.raftis.protocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Incremental parser for client commands in RESP (the Redis wire protocol).
 *
 * <p>Clients normally send each command as an array of bulk strings:
 * {@code *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nhello\r\n}. Tools like telnet send
 * "inline" commands instead: {@code SET key hello\r\n}. Both are supported.
 *
 * <p>TCP gives us a byte stream, not messages, so a command can arrive split across many
 * reads, and one read can contain many commands (pipelining). {@link #parse} therefore
 * returns {@code null} when the buffer holds only part of a command, and never consumes
 * bytes in that case. The caller keeps the leftover bytes and calls again after the next read.
 *
 * <p>Arguments are {@code byte[]}, never {@code String}: Redis keys and values are binary-safe.
 */
public final class RespParser {
    static final int MAX_ARGS = 1024 * 1024;
    static final long MAX_BULK = 512L * 1024 * 1024;
    static final int MAX_INLINE = 64 * 1024;

    private RespParser() {}

    /** One parsed command and the buffer position just after it. */
    public record Result(List<byte[]> args, int next) {}

    /**
     * Parses one command from {@code buf[pos, end)}.
     *
     * @return the command, or {@code null} if more bytes are needed
     * @throws ProtocolException if the bytes can never form a valid command
     */
    public static Result parse(byte[] buf, int pos, int end) {
        if (pos >= end) return null;
        return buf[pos] == '*' ? parseArray(buf, pos, end) : parseInline(buf, pos, end);
    }

    private static Result parseArray(byte[] buf, int pos, int end) {
        int lineEnd = findCrlf(buf, pos + 1, end);
        if (lineEnd < 0) {
            if (end - pos > MAX_INLINE) throw new ProtocolException("too big multibulk count string");
            return null;
        }
        long count = parseLong(buf, pos + 1, lineEnd, "invalid multibulk length");
        if (count > MAX_ARGS) throw new ProtocolException("invalid multibulk length");
        int p = lineEnd + 2;
        if (count <= 0) return new Result(List.of(), p); // "*0" and "*-1" are no-ops, like in Redis

        List<byte[]> args = new ArrayList<>((int) Math.min(count, 16));
        for (long i = 0; i < count; i++) {
            if (p >= end) return null;
            if (buf[p] != '$') throw new ProtocolException("expected '$', got '" + printable(buf[p]) + "'");
            int headerEnd = findCrlf(buf, p + 1, end);
            if (headerEnd < 0) {
                if (end - p > MAX_INLINE) throw new ProtocolException("too big bulk count string");
                return null;
            }
            long len = parseLong(buf, p + 1, headerEnd, "invalid bulk length");
            if (len < 0 || len > MAX_BULK) throw new ProtocolException("invalid bulk length");
            int start = headerEnd + 2;
            if (start + len + 2 > end) return null; // the value hasn't fully arrived yet
            int stop = start + (int) len;
            if (buf[stop] != '\r' || buf[stop + 1] != '\n') {
                throw new ProtocolException("bulk string is not terminated by CRLF");
            }
            args.add(Arrays.copyOfRange(buf, start, stop));
            p = stop + 2;
        }
        return new Result(args, p);
    }

    private static Result parseInline(byte[] buf, int pos, int end) {
        int newline = -1;
        for (int i = pos; i < end; i++) {
            if (buf[i] == '\n') {
                newline = i;
                break;
            }
        }
        if (newline < 0) {
            if (end - pos > MAX_INLINE) throw new ProtocolException("too big inline request");
            return null;
        }
        int lineEnd = newline > pos && buf[newline - 1] == '\r' ? newline - 1 : newline;
        List<byte[]> args = new ArrayList<>();
        int i = pos;
        while (i < lineEnd) {
            while (i < lineEnd && (buf[i] == ' ' || buf[i] == '\t')) i++;
            int start = i;
            while (i < lineEnd && buf[i] != ' ' && buf[i] != '\t') i++;
            if (i > start) args.add(Arrays.copyOfRange(buf, start, i));
        }
        return new Result(args, newline + 1);
    }

    static int findCrlf(byte[] buf, int from, int end) {
        for (int i = from; i + 1 < end; i++) {
            if (buf[i] == '\r' && buf[i + 1] == '\n') return i;
        }
        return -1;
    }

    private static long parseLong(byte[] buf, int from, int to, String error) {
        if (from >= to || to - from > 19) throw new ProtocolException(error);
        boolean negative = buf[from] == '-';
        int i = negative ? from + 1 : from;
        if (i >= to) throw new ProtocolException(error);
        long value = 0;
        for (; i < to; i++) {
            int digit = buf[i] - '0';
            if (digit < 0 || digit > 9) throw new ProtocolException(error);
            value = value * 10 + digit;
        }
        return negative ? -value : value;
    }

    private static String printable(byte b) {
        return b >= 32 && b < 127 ? String.valueOf((char) b) : String.format("\\x%02x", b & 0xff);
    }
}
