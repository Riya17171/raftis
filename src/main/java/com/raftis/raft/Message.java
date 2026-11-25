package com.raftis.raft;

import java.util.List;

/**
 * The messages Raft nodes exchange (Figure 2 of the Raft paper, plus a read-confirmation
 * sequence number used by ReadIndex).
 */
public sealed interface Message {
    int from();

    int to();

    long term();

    /** A candidate asking for a vote. */
    record RequestVote(int from, int to, long term, long lastLogIndex, long lastLogTerm) implements Message {}

    record VoteReply(int from, int to, long term, boolean granted) implements Message {}

    /**
     * Log replication, and a heartbeat when {@code entries} is empty.
     *
     * @param readSeq the leader's latest read-confirmation round. A follower echoes it back, which
     *                proves that this leader was still the leader when the message was sent.
     */
    record AppendEntries(int from, int to, long term, long prevLogIndex, long prevLogTerm,
                         List<Entry> entries, long leaderCommit, long readSeq) implements Message {}

    /**
     * @param matchIndex    on success, the last index the follower now has that matches the leader
     * @param conflictIndex on failure, where the leader should retry from
     * @param conflictTerm  on failure, the term of the follower's conflicting entry (-1 if its log is too short)
     */
    record AppendReply(int from, int to, long term, boolean success, long matchIndex,
                       long conflictIndex, long conflictTerm, long readSeq) implements Message {}

    /** Sent instead of log entries when a follower needs entries the leader has already compacted. */
    record InstallSnapshot(int from, int to, long term, long lastIncludedIndex, long lastIncludedTerm,
                           byte[] data, long readSeq) implements Message {}

    record SnapshotReply(int from, int to, long term, long lastIncludedIndex, long readSeq) implements Message {}
}
