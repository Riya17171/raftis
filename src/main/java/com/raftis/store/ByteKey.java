package com.raftis.store;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * A {@code byte[]} that can be used as a HashMap key.
 *
 * <p>Java arrays use identity for {@code equals}/{@code hashCode}, so two arrays with the same
 * bytes would be different keys. This wrapper compares contents and caches the hash.
 * The wrapped array must never be modified afterwards.
 */
public final class ByteKey implements Comparable<ByteKey> {
    private final byte[] bytes;
    private final int hash;

    public ByteKey(byte[] bytes) {
        this.bytes = bytes;
        this.hash = Arrays.hashCode(bytes);
    }

    public static ByteKey of(String s) {
        return new ByteKey(s.getBytes(StandardCharsets.UTF_8));
    }

    public byte[] bytes() {
        return bytes;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof ByteKey other && hash == other.hash && Arrays.equals(bytes, other.bytes);
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public int compareTo(ByteKey other) {
        return Arrays.compareUnsigned(bytes, other.bytes);
    }

    @Override
    public String toString() {
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
