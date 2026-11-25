package com.raftis.raft;

/**
 * Raft timing and sizing. Times are in ticks; the server ticks every 10 ms.
 *
 * @param electionTicksMin    a follower waits a random time in [min, max) without hearing from a
 *                            leader before starting an election. Randomness makes split votes rare.
 * @param heartbeatTicks      how often a leader sends heartbeats; must be well below the election timeout
 * @param maxEntriesPerMessage cap on entries in one AppendEntries (bounds message size)
 * @param snapshotThreshold   compact the log once this many entries have been applied since the last snapshot
 */
public record RaftConfig(int electionTicksMin, int electionTicksMax, int heartbeatTicks,
                         int maxEntriesPerMessage, long snapshotThreshold) {

    /**
     * 150-300 ms election timeout and 50 ms heartbeats with 10 ms ticks, as in the Raft paper.
     * Snapshots every 100,000 entries: taking one blocks the event loop briefly, so not too often,
     * but often enough that a restarting node doesn't replay a huge log.
     */
    public static RaftConfig defaults() {
        return new RaftConfig(15, 30, 5, 1000, 100_000);
    }
}
