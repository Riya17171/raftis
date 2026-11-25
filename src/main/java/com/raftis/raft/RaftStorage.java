package com.raftis.raft;

import java.util.List;

/**
 * Durable Raft state: the current term and vote, the log, and the latest snapshot.
 *
 * <p>Changes may be buffered. They only have to be on disk after {@link #sync()} returns, and
 * the host must call {@code sync()} before sending any message the node produced. That is how
 * Raft's rule "persist before you reply" is enforced: a vote or an acknowledgement is never
 * sent for state that a crash could still lose.
 */
public interface RaftStorage {
    record HardState(long term, int votedFor) {}

    HardState hardState();

    void setHardState(HardState state);

    /** The latest snapshot, or null if there is none. */
    RaftSnapshot snapshot();

    /** Log entries after the snapshot, in order. Read once at startup. */
    List<Entry> entries();

    void append(Entry entry);

    /** Removes every entry with an index of at least {@code index}. */
    void truncateFrom(long index);

    /** Saves a snapshot and replaces the log with {@code remaining} (the entries after it). */
    void saveSnapshot(RaftSnapshot snapshot, List<Entry> remaining);

    /** Makes every change so far durable. */
    void sync();

    void close();
}
