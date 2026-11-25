package com.raftis.server;

/** How a connection's reads behave in cluster mode. Set with the READMODE command. */
public enum ReadMode {
    /** Linearizable: the leader confirms it's still the leader (ReadIndex) before reading. The default. */
    CONSISTENT,
    /** The leader reads its own memory without checking. Fast, but a leader cut off by a partition can return stale data. */
    LEADER,
    /** Any node, including followers, reads its own memory. Fastest and spreads load, but may be behind. */
    STALE
}
