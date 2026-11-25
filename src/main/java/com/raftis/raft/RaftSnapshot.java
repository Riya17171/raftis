package com.raftis.raft;

/** The state machine's state after applying every entry up to and including {@code index}. */
public record RaftSnapshot(long index, long term, byte[] data) {}
