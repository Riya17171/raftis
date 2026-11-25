package com.raftis.store;

/**
 * The list type: a circular-array deque.
 *
 * <p>LPUSH/RPUSH/LPOP/RPOP need O(1) at both ends, and LINDEX/LRANGE need O(1) access by
 * index. {@code ArrayDeque} has no {@code get(i)}, and {@code ArrayList} makes adding at the
 * front O(n), so this is a small ring buffer whose capacity is always a power of two
 * (so wrapping around is a cheap bit-mask instead of a modulo).
 */
public final class RList {
    private byte[][] items = new byte[8][];
    private int head;
    private int size;

    public int size() {
        return size;
    }

    public void addFirst(byte[] value) {
        growIfFull();
        head = (head - 1) & (items.length - 1);
        items[head] = value;
        size++;
    }

    public void addLast(byte[] value) {
        growIfFull();
        items[(head + size) & (items.length - 1)] = value;
        size++;
    }

    public byte[] pollFirst() {
        if (size == 0) return null;
        byte[] value = items[head];
        items[head] = null;
        head = (head + 1) & (items.length - 1);
        size--;
        return value;
    }

    public byte[] pollLast() {
        if (size == 0) return null;
        int i = (head + size - 1) & (items.length - 1);
        byte[] value = items[i];
        items[i] = null;
        size--;
        return value;
    }

    /** Element at {@code index}, where 0 is the head. The caller checks bounds. */
    public byte[] get(int index) {
        return items[(head + index) & (items.length - 1)];
    }

    private void growIfFull() {
        if (size < items.length) return;
        byte[][] bigger = new byte[items.length * 2][];
        for (int i = 0; i < size; i++) bigger[i] = get(i);
        items = bigger;
        head = 0;
    }
}
