package com.raftis.raft;

/**
 * One slot in the Raft log.
 *
 * @param data the command bytes, or {@code null} for the no-op entry that every new leader
 *             appends at the start of its term
 */
public record Entry(long index, long term, byte[] data) {
    public boolean isNoop() {
        return data == null;
    }
}
