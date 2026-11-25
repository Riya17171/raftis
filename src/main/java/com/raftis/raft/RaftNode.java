package com.raftis.raft;

import com.raftis.raft.Message.AppendEntries;
import com.raftis.raft.Message.AppendReply;
import com.raftis.raft.Message.InstallSnapshot;
import com.raftis.raft.Message.RequestVote;
import com.raftis.raft.Message.SnapshotReply;
import com.raftis.raft.Message.VoteReply;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * One Raft node, written as a pure state machine: no threads, no sockets, no clock.
 *
 * <p>The host (the server's event loop, or the simulation in tests) drives it:
 * <ol>
 *   <li>feed it input: {@link #tick()} every 10 ms, {@link #step} for each received message,
 *       {@link #propose} for client writes, {@link #readIndex} for consistent reads</li>
 *   <li>call {@link RaftStorage#sync()} so the term, vote and log are on disk</li>
 *   <li>call {@link #persisted()} to say so</li>
 *   <li>send everything from {@link #drainOutbox()}</li>
 * </ol>
 * Because all input goes through a few methods and randomness comes from an injected
 * {@link Random}, the same seed always produces the same run. That's what lets the simulation
 * tests replay any failure exactly. The design follows etcd's Raft library.
 *
 * <p>Comments marked "Figure 2" refer to the rules in the Raft paper (Ongaro &amp; Ousterhout, 2014).
 */
public final class RaftNode {
    public enum Role { FOLLOWER, CANDIDATE, LEADER }

    /** Called when a {@link #readIndex} read may proceed, or can't. */
    public interface ReadCallback {
        void ready();

        void failed(String reason);
    }

    private static final class PendingRead {
        final long seq;
        final ReadCallback callback;
        long index = -1;

        PendingRead(long seq, ReadCallback callback) {
            this.seq = seq;
            this.callback = callback;
        }
    }

    private final int id;
    private final int[] peers;
    private final RaftConfig config;
    private final RaftStorage storage;
    private final RaftLog log;
    private final StateMachine stateMachine;
    private final Random random;
    private final List<Message> outbox = new ArrayList<>();

    // Persistent state (Figure 2): saved through storage before any message that depends on it is sent.
    private long term;
    private int votedFor;

    // Volatile state.
    private Role role = Role.FOLLOWER;
    private int leaderId = -1;
    private long commitIndex;
    private long lastApplied;
    private long durableIndex; // our own log is on disk up to here

    private int electionElapsed;
    private int electionTimeout;
    private int heartbeatElapsed;

    // Candidate state.
    private final Set<Integer> votes = new HashSet<>();

    // Leader state (Figure 2), reset at every election win.
    private final Map<Integer, Long> nextIndex = new HashMap<>();
    private final Map<Integer, Long> matchIndex = new HashMap<>();
    private boolean replicationPending;

    // ReadIndex state.
    private final Map<Integer, Long> ackedReadSeq = new HashMap<>();
    private final ArrayDeque<PendingRead> pendingReads = new ArrayDeque<>();
    private long readSeq;
    private boolean readRoundPending;

    public RaftNode(int id, Collection<Integer> peerIds, RaftConfig config, RaftStorage storage,
                    StateMachine stateMachine, Random random) {
        this.id = id;
        this.peers = peerIds.stream().mapToInt(Integer::intValue).toArray();
        this.config = config;
        this.storage = storage;
        this.stateMachine = stateMachine;
        this.random = random;

        RaftStorage.HardState hard = storage.hardState();
        this.term = hard.term();
        this.votedFor = hard.votedFor();
        this.log = new RaftLog(storage);
        RaftSnapshot snapshot = storage.snapshot();
        if (snapshot != null) {
            stateMachine.restore(snapshot.data());
            commitIndex = snapshot.index();
            lastApplied = snapshot.index();
        }
        durableIndex = log.lastIndex(); // whatever we loaded from storage is on disk
        resetElectionTimer();
    }

    // ------------------------------------------------------------------ inputs

    /** Advances logical time by one tick (10 ms in the server). */
    public void tick() {
        if (role == Role.LEADER) {
            if (++heartbeatElapsed >= config.heartbeatTicks()) broadcastAppend();
        } else if (++electionElapsed >= electionTimeout) {
            startElection();
        }
    }

    /** Handles a message from another node. */
    public void step(Message m) {
        if (m.to() != id) return;
        // Figure 2, all servers: if a message has a newer term, adopt it and become a follower.
        if (m.term() > term) {
            boolean fromLeader = m instanceof AppendEntries || m instanceof InstallSnapshot;
            becomeFollower(m.term(), fromLeader ? m.from() : -1);
        }
        switch (m) {
            case RequestVote rv -> onRequestVote(rv);
            case VoteReply vr -> onVoteReply(vr);
            case AppendEntries ae -> onAppendEntries(ae);
            case AppendReply ar -> onAppendReply(ar);
            case InstallSnapshot is -> onInstallSnapshot(is);
            case SnapshotReply sr -> onSnapshotReply(sr);
        }
    }

    /**
     * Appends a client command to the log if this node is the leader.
     *
     * @return the log index the command will be applied at, or -1 if this node isn't the leader.
     *         The command is only committed once {@link StateMachine#apply} is called for that
     *         index with the same term; a new leader may overwrite it if this one loses power.
     */
    public long propose(byte[] command) {
        if (role != Role.LEADER) return -1;
        long index = appendToOwnLog(command);
        replicationPending = true; // sent from drainOutbox(), so one loop pass's proposals go in one message
        return index;
    }

    /**
     * Linearizable read (ReadIndex, Raft thesis section 6.4). Instead of writing reads to the log,
     * the leader:
     * <ol>
     *   <li>records its commit index (once it has committed an entry in its own term)</li>
     *   <li>confirms it is still the leader by hearing back from a majority after the read arrived</li>
     *   <li>waits until it has applied up to the recorded index</li>
     * </ol>
     * Then the callback can read local state. One round of heartbeats confirms every read that
     * arrived before it, so reads are batched for free.
     */
    public void readIndex(ReadCallback callback) {
        if (role != Role.LEADER) {
            callback.failed("not the leader");
            return;
        }
        PendingRead read = new PendingRead(++readSeq, callback);
        if (committedInCurrentTerm()) read.index = commitIndex;
        pendingReads.add(read);
        readRoundPending = true;
        processReads(); // a single-node cluster can answer immediately
    }

    /** The host calls this after {@link RaftStorage#sync()}: our own log is now durable. */
    public void persisted() {
        durableIndex = log.lastIndex();
        if (role == Role.LEADER) maybeCommit();
    }

    /** Messages to send. Call after {@link RaftStorage#sync()} and {@link #persisted()}. */
    public List<Message> drainOutbox() {
        if (role == Role.LEADER && (replicationPending || readRoundPending)) broadcastAppend();
        replicationPending = false;
        readRoundPending = false;
        if (outbox.isEmpty()) return List.of();
        List<Message> out = new ArrayList<>(outbox);
        outbox.clear();
        return out;
    }

    /** Takes a snapshot right now (the SAVE command in cluster mode). */
    public boolean compactNow() {
        if (lastApplied <= log.snapshotIndex()) return false;
        log.compact(lastApplied, log.term(lastApplied), stateMachine.snapshot());
        return true;
    }

    // ------------------------------------------------------------------ elections

    private void startElection() {
        role = Role.CANDIDATE;
        term++;
        votedFor = id;
        persistHardState();
        leaderId = -1;
        votes.clear();
        votes.add(id);
        resetElectionTimer();
        if (votes.size() >= quorum()) {
            becomeLeader(); // single-node cluster
            return;
        }
        for (int peer : peers) {
            send(new RequestVote(id, peer, term, log.lastIndex(), log.lastTerm()));
        }
    }

    private void onRequestVote(RequestVote rv) {
        // Figure 2 + section 5.4.1: grant only if we haven't voted for someone else this term AND the
        // candidate's log is at least as up to date as ours. The second rule is what guarantees a
        // new leader already has every committed entry.
        boolean upToDate = rv.lastLogTerm() > log.lastTerm()
                || (rv.lastLogTerm() == log.lastTerm() && rv.lastLogIndex() >= log.lastIndex());
        boolean grant = rv.term() == term && (votedFor == -1 || votedFor == rv.from()) && upToDate;
        if (grant) {
            if (votedFor != rv.from()) {
                votedFor = rv.from();
                persistHardState();
            }
            resetElectionTimer(); // only reset when granting a vote, not on every message
        }
        send(new VoteReply(id, rv.from(), term, grant));
    }

    private void onVoteReply(VoteReply vr) {
        if (role != Role.CANDIDATE || vr.term() != term || !vr.granted()) return;
        votes.add(vr.from());
        if (votes.size() >= quorum()) becomeLeader();
    }

    private void becomeLeader() {
        role = Role.LEADER;
        leaderId = id;
        heartbeatElapsed = 0;
        electionElapsed = 0;
        for (int peer : peers) {
            nextIndex.put(peer, log.lastIndex() + 1);
            matchIndex.put(peer, 0L);
            ackedReadSeq.put(peer, 0L);
        }
        // A leader may only count replicas for entries from its own term (Figure 8), so entries
        // left over from earlier terms can't commit until something from this term does. A no-op
        // commits them right away, and lets ReadIndex start working.
        appendToOwnLog(null);
        replicationPending = true;
    }

    private void becomeFollower(long newTerm, int leader) {
        if (newTerm > term) {
            term = newTerm;
            votedFor = -1;
            persistHardState();
        }
        boolean wasLeader = role == Role.LEADER;
        role = Role.FOLLOWER;
        leaderId = leader;
        votes.clear();
        if (wasLeader) failPendingReads("leadership lost");
    }

    // ------------------------------------------------------------------ replication (leader)

    private long appendToOwnLog(byte[] data) {
        long index = log.lastIndex() + 1;
        log.append(new Entry(index, term, data));
        return index;
    }

    private void broadcastAppend() {
        heartbeatElapsed = 0;
        for (int peer : peers) sendAppend(peer);
    }

    private void sendAppend(int peer) {
        long next = nextIndex.get(peer);
        if (next <= log.snapshotIndex()) {
            sendSnapshot(peer); // the entries it needs were compacted away
            return;
        }
        long prev = next - 1;
        long last = Math.min(log.lastIndex(), prev + config.maxEntriesPerMessage());
        List<Entry> entries = next <= last ? log.slice(next, last) : List.of();
        send(new AppendEntries(id, peer, term, prev, log.term(prev), entries, commitIndex, readSeq));
        // Optimistic: assume it arrives so the next message carries new entries only. If it's
        // lost, the follower rejects a later message and we move nextIndex back.
        if (!entries.isEmpty()) nextIndex.put(peer, last + 1);
    }

    private void sendSnapshot(int peer) {
        RaftSnapshot snapshot = storage.snapshot();
        send(new InstallSnapshot(id, peer, term, snapshot.index(), snapshot.term(), snapshot.data(), readSeq));
        nextIndex.put(peer, snapshot.index() + 1);
    }

    private void onAppendReply(AppendReply ar) {
        if (role != Role.LEADER || ar.term() != term) return;
        int peer = ar.from();
        ackedReadSeq.merge(peer, ar.readSeq(), Math::max); // even a rejection proves we're still its leader
        if (ar.success()) {
            if (ar.matchIndex() > matchIndex.get(peer)) matchIndex.put(peer, ar.matchIndex());
            if (ar.matchIndex() + 1 > nextIndex.get(peer)) nextIndex.put(peer, ar.matchIndex() + 1);
            maybeCommit();
            if (nextIndex.get(peer) <= log.lastIndex()) sendAppend(peer); // still catching up
        } else {
            // Fast log backtracking (Raft paper end of 5.3, "Students' Guide to Raft"): skip a
            // whole conflicting term per round trip instead of one entry.
            long next;
            if (ar.conflictTerm() >= 0) {
                long lastOfTerm = log.lastIndexOfTerm(ar.conflictTerm());
                next = lastOfTerm >= 0 ? lastOfTerm + 1 : ar.conflictIndex();
            } else {
                next = ar.conflictIndex();
            }
            next = Math.max(next, matchIndex.get(peer) + 1);
            next = Math.min(next, log.lastIndex() + 1);
            nextIndex.put(peer, next);
            sendAppend(peer);
        }
        processReads();
    }

    private void onSnapshotReply(SnapshotReply sr) {
        if (role != Role.LEADER || sr.term() != term) return;
        int peer = sr.from();
        ackedReadSeq.merge(peer, sr.readSeq(), Math::max);
        if (sr.lastIncludedIndex() > matchIndex.get(peer)) matchIndex.put(peer, sr.lastIncludedIndex());
        if (sr.lastIncludedIndex() + 1 > nextIndex.get(peer)) nextIndex.put(peer, sr.lastIncludedIndex() + 1);
        maybeCommit();
        if (nextIndex.get(peer) <= log.lastIndex()) sendAppend(peer);
        processReads();
    }

    /**
     * Commit the highest index that a majority has on disk, but only if that entry is from the
     * current term (Figure 8). Counting replicas of an older term's entry is unsafe: the paper
     * shows such an entry can be stored on a majority and still be overwritten by a later leader.
     */
    private void maybeCommit() {
        long[] matches = new long[peers.length + 1];
        matches[0] = durableIndex;
        for (int i = 0; i < peers.length; i++) matches[i + 1] = matchIndex.get(peers[i]);
        Arrays.sort(matches);
        long majorityIndex = matches[matches.length - quorum()];
        if (majorityIndex > commitIndex && log.term(majorityIndex) == term) {
            commitIndex = majorityIndex;
            applyCommitted();
        }
    }

    // ------------------------------------------------------------------ replication (follower)

    private void onAppendEntries(AppendEntries ae) {
        if (ae.term() < term) {
            send(new AppendReply(id, ae.from(), term, false, 0, 0, -1, ae.readSeq()));
            return;
        }
        if (role == Role.LEADER) {
            throw new IllegalStateException("two leaders in term " + term + ": " + id + " and " + ae.from());
        }
        if (role == Role.CANDIDATE) becomeFollower(term, ae.from()); // someone else won this term
        leaderId = ae.from();
        resetElectionTimer();

        long prev = ae.prevLogIndex();
        long prevTerm = ae.prevLogTerm();
        List<Entry> entries = ae.entries();
        if (prev < log.snapshotIndex()) {
            // The start of this message is already inside our snapshot. Snapshots only hold
            // committed entries, which match the leader's log, so skip that part.
            int skip = (int) Math.min(entries.size(), log.snapshotIndex() - prev);
            entries = entries.subList(skip, entries.size());
            prev = log.snapshotIndex();
            prevTerm = log.snapshotTerm();
        }

        // Figure 2: reject unless our log has an entry at prevLogIndex with prevLogTerm.
        if (prev > log.lastIndex()) {
            send(new AppendReply(id, ae.from(), term, false, 0, log.lastIndex() + 1, -1, ae.readSeq()));
            return;
        }
        if (log.term(prev) != prevTerm) {
            long conflictTerm = log.term(prev);
            long conflictIndex = log.firstIndexOfTermEndingAt(prev);
            send(new AppendReply(id, ae.from(), term, false, 0, conflictIndex, conflictTerm, ae.readSeq()));
            return;
        }

        // Figure 2: if an existing entry conflicts with a new one (same index, different term),
        // delete it and everything after it. Entries that match are kept. A delayed old message
        // must never truncate entries we already have.
        for (Entry e : entries) {
            if (e.index() <= log.lastIndex()) {
                if (log.term(e.index()) == e.term()) continue;
                if (e.index() <= commitIndex) {
                    throw new IllegalStateException("would delete committed entry " + e.index() + " on node " + id);
                }
                log.truncateFrom(e.index());
                durableIndex = Math.min(durableIndex, e.index() - 1);
            }
            log.append(e);
        }

        long lastNew = prev + entries.size();
        long newCommit = Math.min(ae.leaderCommit(), lastNew);
        if (newCommit > commitIndex) {
            commitIndex = newCommit;
            applyCommitted();
        }
        send(new AppendReply(id, ae.from(), term, true, lastNew, 0, -1, ae.readSeq()));
    }

    private void onInstallSnapshot(InstallSnapshot is) {
        if (is.term() < term) {
            send(new SnapshotReply(id, is.from(), term, 0, is.readSeq()));
            return;
        }
        if (role == Role.LEADER) {
            throw new IllegalStateException("two leaders in term " + term + ": " + id + " and " + is.from());
        }
        if (role == Role.CANDIDATE) becomeFollower(term, is.from());
        leaderId = is.from();
        resetElectionTimer();
        // A snapshot older than what we've already committed is stale: ignore it.
        if (is.lastIncludedIndex() > commitIndex) {
            log.installSnapshot(is.lastIncludedIndex(), is.lastIncludedTerm(), is.data());
            stateMachine.restore(is.data());
            commitIndex = is.lastIncludedIndex();
            lastApplied = is.lastIncludedIndex();
            durableIndex = log.lastIndex();
        }
        send(new SnapshotReply(id, is.from(), term, is.lastIncludedIndex(), is.readSeq()));
    }

    // ------------------------------------------------------------------ applying and reads

    private void applyCommitted() {
        while (lastApplied < commitIndex) {
            lastApplied++;
            Entry e = log.get(lastApplied);
            stateMachine.apply(e.index(), e.term(), e.data());
        }
        processReads();
        if (lastApplied - log.snapshotIndex() >= config.snapshotThreshold()) {
            log.compact(lastApplied, log.term(lastApplied), stateMachine.snapshot());
        }
    }

    private boolean committedInCurrentTerm() {
        return log.term(commitIndex) == term;
    }

    private void processReads() {
        if (role != Role.LEADER) return;
        boolean committedInTerm = committedInCurrentTerm();
        while (!pendingReads.isEmpty()) {
            PendingRead read = pendingReads.peek();
            if (read.index < 0) {
                if (!committedInTerm) return; // wait for this term's no-op to commit
                read.index = commitIndex;
            }
            if (!confirmedByMajority(read.seq) || lastApplied < read.index) return;
            pendingReads.poll();
            read.callback.ready();
        }
    }

    private boolean confirmedByMajority(long seq) {
        int acks = 1; // ourselves
        for (int peer : peers) {
            if (ackedReadSeq.getOrDefault(peer, 0L) >= seq) acks++;
        }
        return acks >= quorum();
    }

    private void failPendingReads(String reason) {
        while (!pendingReads.isEmpty()) pendingReads.poll().callback.failed(reason);
    }

    // ------------------------------------------------------------------ helpers

    private void send(Message m) {
        outbox.add(m);
    }

    private void persistHardState() {
        storage.setHardState(new RaftStorage.HardState(term, votedFor));
    }

    private void resetElectionTimer() {
        electionElapsed = 0;
        electionTimeout = config.electionTicksMin()
                + random.nextInt(config.electionTicksMax() - config.electionTicksMin());
    }

    private int quorum() {
        return (peers.length + 1) / 2 + 1;
    }

    // ------------------------------------------------------------------ status

    public int id() {
        return id;
    }

    public Role role() {
        return role;
    }

    public boolean isLeader() {
        return role == Role.LEADER;
    }

    public long term() {
        return term;
    }

    public int leaderId() {
        return leaderId;
    }

    public long commitIndex() {
        return commitIndex;
    }

    public long lastApplied() {
        return lastApplied;
    }

    public long lastLogIndex() {
        return log.lastIndex();
    }

    public long snapshotIndex() {
        return log.snapshotIndex();
    }

    /** How far each follower's log is known to match ours (leader only). */
    public Map<Integer, Long> matchIndexes() {
        return Map.copyOf(matchIndex);
    }
}
