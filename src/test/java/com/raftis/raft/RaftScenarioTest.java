package com.raftis.raft;

import com.raftis.raft.Message.AppendEntries;
import com.raftis.raft.Message.RequestVote;
import com.raftis.raft.Message.VoteReply;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Hand-written scenarios for bugs that need an exact sequence of events, too precise for random
 * simulation to hit reliably. Each test decides exactly which messages get delivered.
 */
class RaftScenarioTest {

    /**
     * A node votes, crashes, and restarts. If the vote wasn't on disk, it could vote again for a
     * different candidate in the same term, and that term could end up with two leaders.
     */
    @Test
    void aVoteSurvivesARestart() {
        Cluster c = new Cluster(3, RaftConfig.defaults());
        c.becomeCandidate(1);
        c.deliver(m -> m instanceof RequestVote && m.to() == 0); // node 0 votes for node 1 only
        c.restart(0);

        c.becomeCandidate(2); // same term as node 1's election
        assertEquals(c.node(1).term(), c.node(2).term());
        c.deliver(m -> m instanceof RequestVote && m.from() == 2 && m.to() == 0);
        VoteReply reply = c.lastVoteReplyFrom(0, 2);
        assertFalse(reply.granted(), "node 0 already voted for node 1 in this term; it must not forget that on restart");
    }

    /**
     * Figure 8 of the Raft paper, step by step (indexes shift by one because every new leader here
     * appends a no-op). The danger: a leader sees an entry from an OLD term stored on a majority
     * and counts it as committed. A later leader can still legitimately overwrite that entry, so a
     * "committed" write disappears. The fix is to only count replicas for entries from the
     * leader's current term.
     */
    @Test
    void figure8_anOldTermEntryOnAMajorityIsNotCommitted() {
        // One entry per message, so the old entry and the new leader's no-op travel separately.
        Cluster c = new Cluster(5, new RaftConfig(15, 30, 5, 1, 10_000));
        Set<Integer> all = Set.of(0, 1, 2, 3, 4);

        // (a) S0 leads term 1, commits its no-op everywhere, then replicates "x" (index 2) to S1 only.
        c.electLeader(0, all);
        c.deliverWithin(all, m -> true);
        c.propose(0, "x");
        c.deliverWithin(Set.of(0, 1), m -> true);
        c.dropAll(); // the copies of "x" headed to S2, S3, S4 are lost

        // (b) S0 crashes. S4 wins term 2 with votes from S2 and S3, appends its no-op at index 2,
        //     and crashes before sending it anywhere.
        c.crash(0);
        c.electLeader(4, Set.of(2, 3, 4));
        c.dropAll();
        c.crash(4);

        // (c) S0 restarts and wins a later term with S1 and S2. It sends its old-term "x" (index 2)
        //     to S2, but S2 never gets the no-op from this term (index 3).
        c.restart(0);
        c.electLeader(0, Set.of(0, 1, 2));
        c.deliverWithin(Set.of(0, 1, 2), m -> !(m instanceof AppendEntries ae && ae.to() == 2
                && c.node(2).lastLogIndex() >= 2 && ae.entries().stream().anyMatch(e -> e.index() >= 3)));
        // "x" from term 1 is now on S0, S1 and S2: a majority...
        assertEquals(2L, c.node(0).matchIndexes().get(2), "setup: S2 should hold index 2");
        assertTrue(c.node(0).matchIndexes().get(1) >= 2, "setup: S1 should hold index 2");
        // ...but it must still NOT count as committed.
        assertTrue(c.node(0).commitIndex() < 2, "an old-term entry was committed by counting replicas (Figure 8)");

        // (d) S0 crashes. S4 restarts and wins with S2 and S3; its log (last term 2) beats theirs
        //     (last term 1), so it legitimately overwrites index 2 everywhere.
        c.crash(0);
        c.restart(4);
        c.electLeader(4, Set.of(2, 3, 4));
        c.deliverWithin(Set.of(2, 3, 4), m -> true);
        assertTrue(c.node(4).commitIndex() >= 3);

        // Everyone recovers and converges. If "x" had been treated as committed at (c), its
        // index would now hold a different entry and the recorder would have thrown.
        c.restart(0);
        c.deliverAfterTicks(all, 50);
        assertEquals(c.node(4).lastApplied(), c.node(0).lastApplied());
    }

    // ------------------------------------------------------------------ harness

    /**
     * A cluster where the test decides which messages arrive. Messages that aren't delivered stay
     * in flight until a later step delivers them or {@link #dropAll()} / {@link #crash} loses them.
     */
    private static final class Cluster {
        private final RaftConfig config;
        private final MemoryRaftStorage[] storage;
        private final RaftNode[] nodes;
        private final boolean[] up;
        private final List<Message> inFlight = new ArrayList<>();
        private final List<Message> sent = new ArrayList<>();
        private final Map<Long, String> appliedAt = new HashMap<>();
        private final Random random = new Random(1);

        Cluster(int size, RaftConfig config) {
            this.config = config;
            storage = new MemoryRaftStorage[size];
            nodes = new RaftNode[size];
            up = new boolean[size];
            for (int i = 0; i < size; i++) {
                storage[i] = new MemoryRaftStorage();
                restart(i);
            }
        }

        RaftNode node(int id) {
            return nodes[id];
        }

        void restart(int id) {
            List<Integer> peers = new ArrayList<>();
            for (int i = 0; i < nodes.length; i++) if (i != id) peers.add(i);
            nodes[id] = new RaftNode(id, peers, config, storage[id], new Recorder(id), new Random(random.nextLong()));
            up[id] = true;
        }

        void crash(int id) {
            up[id] = false;
            inFlight.removeIf(m -> m.to() == id || m.from() == id);
        }

        void becomeCandidate(int id) {
            long before = nodes[id].term();
            for (int i = 0; i < 100 && nodes[id].term() == before; i++) {
                nodes[id].tick();
                afterInput(id);
            }
        }

        void electLeader(int id, Set<Integer> voters) {
            for (int attempt = 0; attempt < 10 && !nodes[id].isLeader(); attempt++) {
                becomeCandidate(id);
                deliverWithin(voters, m -> m instanceof RequestVote || m instanceof VoteReply);
            }
            assertTrue(nodes[id].isLeader(), "node " + id + " failed to win an election");
        }

        void propose(int id, String command) {
            assertTrue(nodes[id].propose(command.getBytes(StandardCharsets.UTF_8)) > 0);
            afterInput(id);
        }

        void deliver(Predicate<Message> allowed) {
            deliverWithin(Set.of(0, 1, 2, 3, 4), allowed);
        }

        /** Delivers matching messages between nodes in {@code group} (including replies) until none are left. */
        void deliverWithin(Set<Integer> group, Predicate<Message> allowed) {
            boolean progress = true;
            while (progress) {
                progress = false;
                List<Message> batch = new ArrayList<>(inFlight);
                inFlight.clear();
                List<Message> held = new ArrayList<>();
                for (Message m : batch) {
                    if (!group.contains(m.from()) || !group.contains(m.to()) || !up[m.to()] || !allowed.test(m)) {
                        held.add(m);
                        continue;
                    }
                    progress = true;
                    nodes[m.to()].step(m);
                    afterInput(m.to());
                }
                inFlight.addAll(0, held);
            }
        }

        void deliverAfterTicks(Set<Integer> group, int ticks) {
            for (int t = 0; t < ticks; t++) {
                for (int id : group) {
                    if (!up[id]) continue;
                    nodes[id].tick();
                    afterInput(id);
                }
                deliverWithin(group, m -> true);
            }
        }

        void dropAll() {
            inFlight.clear();
        }

        VoteReply lastVoteReplyFrom(int from, int to) {
            for (int i = sent.size() - 1; i >= 0; i--) {
                if (sent.get(i) instanceof VoteReply vr && vr.from() == from && vr.to() == to) return vr;
            }
            throw new AssertionError("node " + from + " never answered node " + to);
        }

        private void afterInput(int id) {
            storage[id].sync();
            nodes[id].persisted();
            List<Message> out = nodes[id].drainOutbox();
            sent.addAll(out);
            inFlight.addAll(out);
        }

        /** Throws if two nodes ever apply different entries at the same index. */
        private final class Recorder implements StateMachine {
            private final int id;

            Recorder(int id) {
                this.id = id;
            }

            @Override
            public void apply(long index, long term, byte[] command) {
                String entry = term + ":" + (command == null ? "noop" : new String(command, StandardCharsets.UTF_8));
                String first = appliedAt.putIfAbsent(index, entry);
                if (first != null && !first.equals(entry)) {
                    throw new AssertionError("node " + id + " applied " + entry + " at index " + index + " but another node applied " + first);
                }
            }

            @Override
            public byte[] snapshot() {
                return new byte[0];
            }

            @Override
            public void restore(byte[] snapshot) {
                // not used: these scenarios never compact
            }
        }
    }
}
