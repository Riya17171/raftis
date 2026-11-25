package com.raftis.raft;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;

/**
 * A deterministic simulation of a Raft cluster: simulated time, a simulated network that
 * drops, duplicates, delays and reorders messages, plus crashes, restarts and network
 * partitions, all driven by one seeded {@link Random}. The same seed always replays the exact
 * same run, so any failure can be reproduced and debugged. (FoundationDB is the well-known
 * example of testing this way.)
 *
 * <p>While it runs, it checks Raft's safety properties and throws on the first violation:
 * <ul>
 *   <li><b>Election Safety</b>: at most one leader per term</li>
 *   <li><b>State Machine Safety</b>: no two nodes ever apply different entries at the same index</li>
 *   <li><b>Linearizable reads</b>: a ReadIndex read sees every write acknowledged before it started</li>
 *   <li><b>Durability</b>: every acknowledged write is in the final state, whatever crashed</li>
 *   <li><b>Liveness</b>: once failures stop, the cluster elects a leader and commits again</li>
 * </ul>
 */
final class Simulation {
    private static final int TICK_MS = 10;

    private record Event(long time, long seq, Runnable action) {}

    private record Proposal(long term, String command) {}

    final long seed;
    private final Random rnd;
    private final RaftConfig config;
    private final SimNode[] nodes;
    private final boolean[][] cut; // cut[a][b]: messages from a to b are lost
    private final PriorityQueue<Event> events =
            new PriorityQueue<>(Comparator.comparingLong(Event::time).thenComparingLong(Event::seq));
    private long now;
    private long eventSeq;

    double dropRate = 0.05;
    double duplicateRate = 0.02;
    int maxDelayMs = 20;

    // What the checks compare against.
    private final Map<Long, Integer> leaderOfTerm = new HashMap<>();
    private final Map<Long, String> entryAt = new HashMap<>(); // index -> "term:command", first time anyone applied it
    private final Set<String> acknowledged = new LinkedHashSet<>();
    private int nextCommand;
    private boolean clientsActive = true;
    int readsServed;
    int faultsInjected;

    Simulation(long seed, int clusterSize, RaftConfig config) {
        this.seed = seed;
        this.rnd = new Random(seed);
        this.config = config;
        this.nodes = new SimNode[clusterSize];
        this.cut = new boolean[clusterSize][clusterSize];
        for (int i = 0; i < clusterSize; i++) {
            nodes[i] = new SimNode(i);
            nodes[i].start();
            int id = i;
            schedule(1 + rnd.nextInt(TICK_MS), () -> tickLoop(id));
        }
    }

    // ------------------------------------------------------------ scenario

    /** Random client traffic and failures for {@code faultyMs}, then heal everything and check. */
    void run(long faultyMs, boolean injectFaults) {
        scheduleClientLoop();
        scheduleReadLoop();
        if (injectFaults) scheduleFaultLoop(faultyMs);
        runUntil(faultyMs);

        // Heal: reconnect everything, restart crashed nodes, stop losing messages.
        injectFaultsUntil = 0;
        for (boolean[] row : cut) Arrays.fill(row, false);
        for (SimNode n : nodes) if (!n.up) n.start();
        dropRate = 0;
        duplicateRate = 0;
        runUntil(now + 3000);
        clientsActive = false; // let in-flight traffic settle so final states can be compared

        // Liveness: a leader exists and a brand-new command commits everywhere.
        SimNode leader = currentLeader();
        if (leader == null) fail("no leader 3 s after all failures stopped");
        String last = "final-" + seed;
        long index = leader.raft.propose(last.getBytes(StandardCharsets.UTF_8));
        if (index < 0) fail("leader refused a proposal");
        leader.proposals.put(index, new Proposal(leader.raft.term(), last));
        afterInput(leader);
        runUntil(now + 2000);
        if (!acknowledged.contains(last)) fail("final command was not committed within 2 s");

        // Every node converged to the same state, and no acknowledged write was lost.
        List<String> reference = nodes[0].state;
        for (SimNode n : nodes) {
            if (!n.state.equals(reference)) {
                fail("node " + n.id + " applied " + n.state.size() + " entries but node 0 applied " + reference.size());
            }
        }
        Set<String> finalCommands = new HashSet<>(nodes[0].commands);
        for (String cmd : acknowledged) {
            if (!finalCommands.contains(cmd)) fail("acknowledged write " + cmd + " was lost");
        }
    }

    int acknowledgedCount() {
        return acknowledged.size();
    }

    long finalTerm() {
        return Arrays.stream(nodes).mapToLong(n -> n.raft.term()).max().orElse(0);
    }

    // ------------------------------------------------------------ event loop

    private void schedule(long delayMs, Runnable action) {
        events.add(new Event(now + delayMs, eventSeq++, action));
    }

    private void runUntil(long time) {
        while (!events.isEmpty() && events.peek().time() <= time) {
            Event e = events.poll();
            now = e.time();
            e.action().run();
        }
        now = time;
    }

    private void tickLoop(int id) {
        SimNode n = nodes[id];
        if (n.up) {
            n.raft.tick();
            afterInput(n);
        }
        schedule(TICK_MS, () -> tickLoop(id));
    }

    /** What the server's event loop does after feeding the node input: sync, then send. */
    private void afterInput(SimNode n) {
        n.storage.sync();
        n.raft.persisted();
        checkElectionSafety(n);
        for (Message m : n.raft.drainOutbox()) send(m);
    }

    // ------------------------------------------------------------ network

    private void send(Message m) {
        if (cut[m.from()][m.to()] || rnd.nextDouble() < dropRate) return;
        int copies = rnd.nextDouble() < duplicateRate ? 2 : 1;
        for (int i = 0; i < copies; i++) {
            schedule(1 + rnd.nextInt(maxDelayMs), () -> deliver(m)); // random delays also reorder messages
        }
    }

    private void deliver(Message m) {
        SimNode target = nodes[m.to()];
        if (!target.up || cut[m.from()][m.to()]) return;
        target.raft.step(m);
        afterInput(target);
    }

    // ------------------------------------------------------------ clients

    private void scheduleClientLoop() {
        schedule(2 + rnd.nextInt(8), () -> {
            if (!clientsActive) return;
            SimNode leader = randomLeader(); // may be a stale leader cut off in a minority
            if (leader != null) {
                String cmd = "c" + (nextCommand++);
                long index = leader.raft.propose(cmd.getBytes(StandardCharsets.UTF_8));
                if (index > 0) leader.proposals.put(index, new Proposal(leader.raft.term(), cmd));
                afterInput(leader);
            }
            scheduleClientLoop();
        });
    }

    private void scheduleReadLoop() {
        schedule(5 + rnd.nextInt(30), () -> {
            if (!clientsActive) return;
            SimNode leader = randomLeader();
            if (leader != null) {
                Set<String> mustSee = new HashSet<>(acknowledged); // acknowledged before this read started
                SimNode reader = leader;
                leader.raft.readIndex(new RaftNode.ReadCallback() {
                    @Override
                    public void ready() {
                        for (String cmd : mustSee) {
                            if (!reader.commands.contains(cmd)) {
                                fail("stale read on node " + reader.id + ": missing acknowledged write " + cmd);
                            }
                        }
                        readsServed++;
                    }

                    @Override
                    public void failed(String reason) {
                        // fine: the client would retry against the new leader
                    }
                });
                afterInput(leader);
            }
            scheduleReadLoop();
        });
    }

    // ------------------------------------------------------------ faults

    private long injectFaultsUntil;

    private void scheduleFaultLoop(long until) {
        injectFaultsUntil = until;
        schedule(100 + rnd.nextInt(600), this::injectFault);
    }

    private void injectFault() {
        if (now >= injectFaultsUntil) return;
        faultsInjected++;
        int n = nodes.length;
        switch (rnd.nextInt(7)) {
            case 0, 1 -> { // crash a node (it loses everything that isn't in storage)
                SimNode victim = nodes[rnd.nextInt(n)];
                if (victim.up) victim.crash();
            }
            case 6 -> { // quick reboot: crash and come back within 1-50 ms, often mid-election
                SimNode victim = nodes[rnd.nextInt(n)];
                if (victim.up) {
                    victim.crash();
                    schedule(1 + rnd.nextInt(50), () -> {
                        if (!victim.up) victim.start();
                    });
                }
            }
            case 2 -> { // restart a crashed node
                for (SimNode node : nodes) {
                    if (!node.up) {
                        node.start();
                        break;
                    }
                }
            }
            case 3 -> { // split the cluster into two groups that can't talk
                boolean[] side = new boolean[n];
                for (int i = 0; i < n; i++) side[i] = rnd.nextBoolean();
                for (int a = 0; a < n; a++) {
                    for (int b = 0; b < n; b++) cut[a][b] = side[a] != side[b];
                }
            }
            case 4 -> { // one-way link failure
                int a = rnd.nextInt(n), b = rnd.nextInt(n);
                if (a != b) cut[a][b] = true;
            }
            default -> { // heal the network
                for (boolean[] row : cut) Arrays.fill(row, false);
            }
        }
        schedule(100 + rnd.nextInt(600), this::injectFault);
    }

    // ------------------------------------------------------------ checks

    private void checkElectionSafety(SimNode n) {
        if (n.raft.role() != RaftNode.Role.LEADER) return;
        Integer previous = leaderOfTerm.putIfAbsent(n.raft.term(), n.id);
        if (previous != null && previous != n.id) {
            fail("Election Safety violated: nodes " + previous + " and " + n.id + " both led term " + n.raft.term());
        }
    }

    private SimNode randomLeader() {
        List<SimNode> leaders = new ArrayList<>();
        for (SimNode n : nodes) if (n.up && n.raft.isLeader()) leaders.add(n);
        return leaders.isEmpty() ? null : leaders.get(rnd.nextInt(leaders.size()));
    }

    private SimNode currentLeader() {
        SimNode best = null;
        for (SimNode n : nodes) {
            if (n.up && n.raft.isLeader() && (best == null || n.raft.term() > best.raft.term())) best = n;
        }
        return best;
    }

    private void fail(String message) {
        throw new AssertionError("[seed " + seed + ", t=" + now + "ms] " + message);
    }

    private List<Integer> peersOf(int id) {
        List<Integer> peers = new ArrayList<>();
        for (int i = 0; i < nodes.length; i++) if (i != id) peers.add(i);
        return peers;
    }

    // ------------------------------------------------------------ a simulated server

    /** One node: Raft plus a trivial state machine that records every entry it applies, in order. */
    private final class SimNode implements StateMachine {
        final int id;
        final MemoryRaftStorage storage = new MemoryRaftStorage();
        RaftNode raft;
        boolean up;
        List<String> state = new ArrayList<>();      // state.get(i) is the entry applied at index i + 1
        Set<String> commands = new HashSet<>();      // client commands in state, for read checks
        final Map<Long, Proposal> proposals = new HashMap<>();

        SimNode(int id) {
            this.id = id;
        }

        void start() {
            state = new ArrayList<>();
            commands = new HashSet<>();
            proposals.clear();
            raft = new RaftNode(id, peersOf(id), config, storage, this, new Random(rnd.nextLong()));
            up = true;
        }

        void crash() {
            up = false;
            proposals.clear(); // the client's connection broke: outcome unknown, not acknowledged
        }

        @Override
        public void apply(long index, long term, byte[] command) {
            String cmd = command == null ? "noop" : new String(command, StandardCharsets.UTF_8);
            String entry = term + ":" + cmd;
            if (index != state.size() + 1) {
                fail("node " + id + " applied index " + index + " but its next index was " + (state.size() + 1));
            }
            String first = entryAt.putIfAbsent(index, entry);
            if (first != null && !first.equals(entry)) {
                fail("State Machine Safety violated at index " + index + ": " + first + " vs " + entry + " on node " + id);
            }
            state.add(entry);
            if (command != null) commands.add(cmd);
            Proposal p = proposals.remove(index);
            if (p != null && p.term() == term && p.command().equals(cmd)) acknowledged.add(cmd);
        }

        @Override
        public byte[] snapshot() {
            return String.join("\n", state).getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public void restore(byte[] snapshot) {
            String s = new String(snapshot, StandardCharsets.UTF_8);
            state = s.isEmpty() ? new ArrayList<>() : new ArrayList<>(List.of(s.split("\n")));
            commands = new HashSet<>();
            for (String entry : state) {
                String cmd = entry.substring(entry.indexOf(':') + 1);
                if (!cmd.equals("noop")) commands.add(cmd);
            }
        }
    }
}
