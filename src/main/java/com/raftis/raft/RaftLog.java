package com.raftis.raft;

import java.util.ArrayList;
import java.util.List;

/**
 * The in-memory view of the Raft log, kept in step with {@link RaftStorage}.
 *
 * <p>After a snapshot, entries up to {@code snapshotIndex} are gone, so every method works with
 * absolute Raft indexes and hides the offset. Off-by-one errors after compaction are one of the
 * most common Raft bugs, so all index arithmetic lives here.
 */
final class RaftLog {
    private final RaftStorage storage;
    private final ArrayList<Entry> entries = new ArrayList<>(); // entries.get(i).index() == snapshotIndex + 1 + i
    private long snapshotIndex;
    private long snapshotTerm;

    RaftLog(RaftStorage storage) {
        this.storage = storage;
        RaftSnapshot snapshot = storage.snapshot();
        if (snapshot != null) {
            snapshotIndex = snapshot.index();
            snapshotTerm = snapshot.term();
        }
        for (Entry e : storage.entries()) {
            if (e.index() != lastIndex() + 1) {
                throw new IllegalStateException("stored log is not contiguous at index " + e.index());
            }
            entries.add(e);
        }
    }

    long snapshotIndex() {
        return snapshotIndex;
    }

    long snapshotTerm() {
        return snapshotTerm;
    }

    long lastIndex() {
        return snapshotIndex + entries.size();
    }

    long lastTerm() {
        return entries.isEmpty() ? snapshotTerm : entries.get(entries.size() - 1).term();
    }

    /** The term of the entry at {@code index}, or -1 if it is compacted away or doesn't exist yet. */
    long term(long index) {
        if (index == snapshotIndex) return snapshotTerm;
        if (index < snapshotIndex || index > lastIndex()) return -1;
        return entries.get(offset(index)).term();
    }

    Entry get(long index) {
        if (index <= snapshotIndex || index > lastIndex()) {
            throw new IllegalArgumentException("no entry " + index + " (log covers " + (snapshotIndex + 1) + ".." + lastIndex() + ")");
        }
        return entries.get(offset(index));
    }

    /** A copy of the entries in [from, to]. A copy, because the log may be truncated later. */
    List<Entry> slice(long from, long to) {
        return new ArrayList<>(entries.subList(offset(from), offset(to) + 1));
    }

    void append(Entry entry) {
        if (entry.index() != lastIndex() + 1) {
            throw new IllegalStateException("appending index " + entry.index() + " after " + lastIndex());
        }
        entries.add(entry);
        storage.append(entry);
    }

    /** Deletes the entry at {@code index} and everything after it. */
    void truncateFrom(long index) {
        if (index <= snapshotIndex) throw new IllegalStateException("can't truncate into the snapshot at " + index);
        if (index > lastIndex()) return;
        entries.subList(offset(index), entries.size()).clear();
        storage.truncateFrom(index);
    }

    /** The first index of the run of entries with {@code term} that ends at {@code index}. */
    long firstIndexOfTermEndingAt(long index) {
        long term = term(index);
        long i = index;
        while (i - 1 > snapshotIndex && term(i - 1) == term) i--;
        return i;
    }

    /** The last index holding an entry from {@code term}, or -1 if the log has none. */
    long lastIndexOfTerm(long term) {
        for (long i = lastIndex(); i > snapshotIndex; i--) {
            long t = term(i);
            if (t == term) return i;
            if (t < term) return -1; // terms only increase along the log
        }
        return -1;
    }

    /** Drops entries up to {@code index}, which the state machine snapshot now covers. */
    void compact(long index, long term, byte[] data) {
        List<Entry> remaining = new ArrayList<>(entries.subList(offset(index) + 1, entries.size()));
        replaceWithSnapshot(index, term, data, remaining);
    }

    /**
     * Installs a snapshot received from the leader. If our log already has the snapshot's last
     * entry, the entries after it are still valid and are kept; otherwise the whole log is replaced.
     */
    void installSnapshot(long index, long term, byte[] data) {
        List<Entry> remaining = term(index) == term && index <= lastIndex()
                ? new ArrayList<>(entries.subList(offset(index) + 1, entries.size()))
                : new ArrayList<>();
        replaceWithSnapshot(index, term, data, remaining);
    }

    private void replaceWithSnapshot(long index, long term, byte[] data, List<Entry> remaining) {
        storage.saveSnapshot(new RaftSnapshot(index, term, data), remaining);
        entries.clear();
        entries.addAll(remaining);
        snapshotIndex = index;
        snapshotTerm = term;
    }

    private int offset(long index) {
        return (int) (index - snapshotIndex - 1);
    }
}
