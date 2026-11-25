package com.raftis.protocol;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/**
 * A growable byte buffer that encodes RESP replies.
 *
 * <p>Each client connection owns one. Commands append replies to it, and the event loop
 * sends its contents to the socket. The append-only file uses one too, to encode commands.
 */
public final class RespWriter {
    private static final int SHRINK_ABOVE = 1024 * 1024;
    private static final byte[] CRLF = {'\r', '\n'};
    private static final byte[] NULL_BULK = "$-1\r\n".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] NULL_ARRAY = "*-1\r\n".getBytes(StandardCharsets.US_ASCII);

    private final int initialCapacity;
    private byte[] buf;
    private int len;

    public RespWriter() {
        this(256);
    }

    public RespWriter(int initialCapacity) {
        this.initialCapacity = initialCapacity;
        this.buf = new byte[initialCapacity];
    }

    /** {@code +OK\r\n} */
    public RespWriter simple(String s) {
        put((byte) '+');
        put(sanitize(s).getBytes(StandardCharsets.UTF_8));
        put(CRLF);
        return this;
    }

    /** {@code -ERR message\r\n}. The message should start with an error code such as ERR or WRONGTYPE. */
    public RespWriter error(String message) {
        put((byte) '-');
        put(sanitize(message).getBytes(StandardCharsets.UTF_8));
        put(CRLF);
        return this;
    }

    /** {@code :42\r\n} */
    public RespWriter integer(long value) {
        put((byte) ':');
        putAscii(Long.toString(value));
        put(CRLF);
        return this;
    }

    /** {@code $5\r\nhello\r\n}, or a null bulk string if {@code value} is null. */
    public RespWriter bulk(byte[] value) {
        if (value == null) return nullBulk();
        put((byte) '$');
        putAscii(Integer.toString(value.length));
        put(CRLF);
        put(value);
        put(CRLF);
        return this;
    }

    public RespWriter bulk(String value) {
        return bulk(value == null ? null : value.getBytes(StandardCharsets.UTF_8));
    }

    public RespWriter nullBulk() {
        put(NULL_BULK);
        return this;
    }

    /** {@code *3\r\n}: the caller then writes that many elements. */
    public RespWriter arrayHeader(int count) {
        put((byte) '*');
        putAscii(Integer.toString(count));
        put(CRLF);
        return this;
    }

    public RespWriter nullArray() {
        put(NULL_ARRAY);
        return this;
    }

    /** An array of bulk strings. Null elements become null bulk strings. */
    public RespWriter bulkArray(List<byte[]> items) {
        arrayHeader(items.size());
        for (byte[] item : items) bulk(item);
        return this;
    }

    /** Encodes a command the way a client would send it. Used by the append-only file. */
    public RespWriter command(List<byte[]> args) {
        return bulkArray(args);
    }

    public int length() {
        return len;
    }

    /** The backing array. Only the first {@link #length()} bytes are meaningful. */
    public byte[] buffer() {
        return buf;
    }

    public byte[] toByteArray() {
        return Arrays.copyOf(buf, len);
    }

    /** Drops everything after {@code newLength}. Used to discard a half-written reply when a command fails. */
    public void truncate(int newLength) {
        if (newLength < 0 || newLength > len) throw new IllegalArgumentException("bad length " + newLength);
        len = newLength;
    }

    /** Empties the buffer, releasing memory if one huge reply made it grow. */
    public void reset() {
        len = 0;
        if (buf.length > SHRINK_ABOVE) buf = new byte[initialCapacity];
    }

    private void put(byte b) {
        ensure(1);
        buf[len++] = b;
    }

    private void put(byte[] bytes) {
        ensure(bytes.length);
        System.arraycopy(bytes, 0, buf, len, bytes.length);
        len += bytes.length;
    }

    private void putAscii(String s) {
        ensure(s.length());
        for (int i = 0; i < s.length(); i++) buf[len++] = (byte) s.charAt(i);
    }

    private void ensure(int extra) {
        if (len + extra > buf.length) {
            buf = Arrays.copyOf(buf, Math.max(buf.length * 2, len + extra));
        }
    }

    /** Simple strings and errors can't contain CR or LF, or they'd break the protocol framing. */
    private static String sanitize(String s) {
        return s.indexOf('\r') < 0 && s.indexOf('\n') < 0 ? s : s.replace('\r', ' ').replace('\n', ' ');
    }
}
