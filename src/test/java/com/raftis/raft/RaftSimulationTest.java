package com.raftis.raft;

import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs many randomized simulations. Each seed is a different random scenario of crashes,
 * partitions and message loss. Run more with: {@code ./gradlew test -Draftis.simSeeds=10000}
 *
 * <p>If a seed fails, the error message names it; rerun just that seed with
 * {@code -Draftis.simSeed=<seed>} to debug a perfectly reproducible failure.
 */
class RaftSimulationTest {

    @Test
    void safetyHoldsUnderCrashesPartitionsAndMessageLoss() {
        Long only = Long.getLong("raftis.simSeed");
        int seeds = Integer.getInteger("raftis.simSeeds", 300);
        long firstSeed = only != null ? only : 1;
        long lastSeed = only != null ? only : seeds;
        long acknowledged = 0, reads = 0, faults = 0;
        for (long seed = firstSeed; seed <= lastSeed; seed++) {
            Simulation sim = randomScenario(seed);
            sim.run(8_000, true);
            acknowledged += sim.acknowledgedCount();
            reads += sim.readsServed;
            faults += sim.faultsInjected;
        }
        System.out.printf("simulated %d clusters: %d acknowledged writes, %d linearizable reads, %d faults injected%n",
                lastSeed - firstSeed + 1, acknowledged, reads, faults);
        assertTrue(acknowledged > 0);
    }

    @Test
    void healthyClusterElectsALeaderAndCommits() {
        Simulation sim = new Simulation(7, 3, RaftConfig.defaults());
        sim.dropRate = 0;
        sim.duplicateRate = 0;
        sim.run(2_000, false);
        assertTrue(sim.acknowledgedCount() > 100, "only " + sim.acknowledgedCount() + " writes committed");
        assertTrue(sim.finalTerm() <= 2, "a healthy cluster shouldn't need many elections; term was " + sim.finalTerm());
    }

    @Test
    void singleNodeClusterWorks() {
        Simulation sim = new Simulation(11, 1, RaftConfig.defaults());
        sim.run(1_000, false);
        assertTrue(sim.acknowledgedCount() > 50);
    }

    /** Varies cluster size, batch size and snapshot frequency, so compaction and InstallSnapshot get exercised. */
    private static Simulation randomScenario(long seed) {
        Random r = new Random(seed * 31 + 7);
        int size = seed % 2 == 0 ? 3 : 5;
        RaftConfig config = new RaftConfig(15, 30, 5, 1 + r.nextInt(64), 10 + r.nextInt(150));
        return new Simulation(seed, size, config);
    }
}
