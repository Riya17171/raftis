package com.raftis.raft;

/**
 * What Raft replicates into. Raft guarantees every node calls {@link #apply} with the same
 * entries in the same order, so if {@code apply} is deterministic, every node ends up in the same state.
 *
 * <p>These methods are called from inside {@link RaftNode}; they must not call back into it.
 */
public interface StateMachine {
    /** Applies a committed entry. {@code command} is null for a leader's no-op entry. */
    void apply(long index, long term, byte[] command);

    /** Serializes the entire current state. */
    byte[] snapshot();

    /** Replaces the entire state with a snapshot. */
    void restore(byte[] snapshot);
}
