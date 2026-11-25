package com.raftis.raft;

import java.util.ArrayList;
import java.util.List;

/** In-memory storage, used by the simulation tests. A "crash" keeps this object and rebuilds the node. */
public final class MemoryRaftStorage implements RaftStorage {
    private HardState hardState = new HardState(0, -1);
    private RaftSnapshot snapshot;
    private final List<Entry> entries = new ArrayList<>();

    @Override
    public HardState hardState() {
        return hardState;
    }

    @Override
    public void setHardState(HardState state) {
        this.hardState = state;
    }

    @Override
    public RaftSnapshot snapshot() {
        return snapshot;
    }

    @Override
    public List<Entry> entries() {
        return new ArrayList<>(entries);
    }

    @Override
    public void append(Entry entry) {
        entries.add(entry);
    }

    @Override
    public void truncateFrom(long index) {
        entries.removeIf(e -> e.index() >= index);
    }

    @Override
    public void saveSnapshot(RaftSnapshot snapshot, List<Entry> remaining) {
        this.snapshot = snapshot;
        entries.clear();
        entries.addAll(remaining);
    }

    @Override
    public void sync() {
        // nothing to do: memory is our "disk"
    }

    @Override
    public void close() {
        // nothing to do
    }
}
