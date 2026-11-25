package com.raftis.chaos;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Injects one fault at a time, at random: hold it for a few seconds, heal everything, let the
 * cluster recover for a moment, repeat. Every action is logged with a timestamp so a failure can
 * be lined up against what the network and processes were doing.
 */
final class Nemesis implements Runnable {
    enum Fault {
        /** The leader can't talk to anyone, and nobody can talk to it. */
        ISOLATE_LEADER,
        /** A random minority | majority split. */
        SPLIT,
        /** Two halves that can't see each other, joined by one node that sees both. */
        BRIDGE,
        /** One node's messages stop arriving anywhere, but it still hears everyone. */
        ONE_WAY,
        /** SIGKILL the leader. */
        KILL_LEADER,
        /** SIGKILL a random node. */
        KILL_RANDOM,
        /** SIGKILL a majority at once: the cluster must stop, then recover from disk. */
        KILL_MAJORITY,
        /** SIGSTOP the leader: it freezes, a new leader is elected, then it wakes up still thinking it leads. */
        PAUSE_LEADER,
        /** SIGSTOP a random node. */
        PAUSE_RANDOM
    }

    private final ChaosCluster cluster;
    private final Random rnd;
    private final LongSupplier clock; // nanoseconds since the test started
    private final List<String> log = new CopyOnWriteArrayList<>();
    private final Map<Fault, Integer> counts = new EnumMap<>(Fault.class);
    private volatile boolean stopping;
    private Throwable failure;

    Nemesis(ChaosCluster cluster, Random rnd, LongSupplier clock) {
        this.cluster = cluster;
        this.rnd = rnd;
        this.clock = clock;
    }

    @Override
    public void run() {
        try {
            while (!stopping) {
                pauseFor(1000 + rnd.nextInt(1500)); // healthy period
                if (stopping) break;
                Fault fault = Fault.values()[rnd.nextInt(Fault.values().length)];
                inject(fault);
                counts.merge(fault, 1, Integer::sum);
                pauseFor(1000 + rnd.nextInt(3000)); // the fault lasts 1-4 s
                cluster.healEverything();
                note("heal: network healed, every node running");
            }
        } catch (Throwable t) {
            failure = t;
        }
    }

    void stop() {
        stopping = true;
    }

    Throwable failure() {
        return failure;
    }

    List<String> log() {
        return log;
    }

    Map<Fault, Integer> counts() {
        return counts;
    }

    private void inject(Fault fault) throws Exception {
        List<Integer> nodes = IntStream.rangeClosed(1, cluster.size).boxed().collect(Collectors.toCollection(ArrayList::new));
        Collections.shuffle(nodes, rnd);
        int majority = cluster.size / 2 + 1;
        switch (fault) {
            case ISOLATE_LEADER -> {
                int leader = leaderOr(nodes.get(0));
                List<Integer> rest = new ArrayList<>(nodes);
                rest.remove(Integer.valueOf(leader));
                cluster.network.partition(List.of(Set.of(leader), Set.copyOf(rest)));
                note("partition: leader %d isolated from %s", leader, rest);
            }
            case SPLIT -> {
                Set<Integer> minority = Set.copyOf(nodes.subList(0, cluster.size - majority));
                Set<Integer> rest = Set.copyOf(nodes.subList(cluster.size - majority, cluster.size));
                cluster.network.partition(List.of(minority, rest));
                note("partition: %s | %s", sorted(minority), sorted(rest));
            }
            case BRIDGE -> {
                int mid = cluster.size / 2;
                Set<Integer> left = Set.copyOf(nodes.subList(0, mid + 1));
                Set<Integer> right = Set.copyOf(nodes.subList(mid, cluster.size));
                cluster.network.partition(List.of(left, right));
                note("bridge: %s | %s, node %d sees both sides", sorted(left), sorted(right), nodes.get(mid));
            }
            case ONE_WAY -> {
                int node = rnd.nextBoolean() ? leaderOr(nodes.get(0)) : nodes.get(0);
                for (int other : nodes) if (other != node) cluster.network.cut(node, other);
                note("one-way: node %d can hear everyone but nothing it sends arrives", node);
            }
            case KILL_LEADER -> {
                int leader = leaderOr(nodes.get(0));
                cluster.kill(leader);
                note("kill -9 leader %d", leader);
            }
            case KILL_RANDOM -> {
                cluster.kill(nodes.get(0));
                note("kill -9 node %d", nodes.get(0));
            }
            case KILL_MAJORITY -> {
                List<Integer> victims = nodes.subList(0, majority);
                for (int id : victims) cluster.kill(id);
                note("kill -9 majority %s: the cluster must stop accepting writes", sorted(Set.copyOf(victims)));
            }
            case PAUSE_LEADER -> {
                int leader = leaderOr(nodes.get(0));
                cluster.pause(leader);
                note("SIGSTOP leader %d (frozen, like a long GC pause)", leader);
            }
            case PAUSE_RANDOM -> {
                cluster.pause(nodes.get(0));
                note("SIGSTOP node %d", nodes.get(0));
            }
        }
    }

    private int leaderOr(int fallback) {
        int leader = cluster.leader();
        return leader > 0 ? leader : fallback;
    }

    private void pauseFor(long ms) throws InterruptedException {
        long end = System.currentTimeMillis() + ms;
        while (!stopping && System.currentTimeMillis() < end) Thread.sleep(Math.min(50, end - System.currentTimeMillis() + 1));
    }

    void note(String format, Object... args) {
        log.add(String.format("%8.3fs  %s", clock.getAsLong() / 1e9, String.format(format, args)));
    }

    private static List<Integer> sorted(Set<Integer> s) {
        return s.stream().sorted().toList();
    }
}
