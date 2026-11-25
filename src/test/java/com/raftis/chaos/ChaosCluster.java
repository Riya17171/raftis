package com.raftis.chaos;

import com.raftis.playground.LocalCluster;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/** A {@link LocalCluster} for tests, plus leader detection through a real Redis client. */
final class ChaosCluster extends LocalCluster {

    ChaosCluster(int size, Path dir, List<String> extraArgs) throws IOException {
        super(size, dir, System.getProperty("raftis.serverClasspath", System.getProperty("java.class.path")), 0, extraArgs);
    }

    /**
     * The node that claims to be leader with the highest term, or -1. (A leader cut off from the
     * others may still think it leads, in an older term; the real one has the highest term.)
     */
    int leader() {
        int best = -1;
        long bestTerm = -1;
        for (int id = 1; id <= size(); id++) {
            if (!isUp(id) || isPaused(id)) continue;
            String info = info(id);
            if (info == null || !info.contains("role:leader")) continue;
            long term = Long.parseLong(field(info, "term"));
            if (term > bestTerm) {
                best = id;
                bestTerm = term;
            }
        }
        return best;
    }

    int awaitLeader(long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            int leader = leader();
            if (leader > 0) return leader;
            Thread.sleep(25);
        }
        return -1;
    }

    /** INFO from a node, or null if it doesn't answer quickly. */
    String info(int id) {
        try (Jedis j = new Jedis("127.0.0.1", clientPort(id), 300)) {
            return j.info();
        } catch (RuntimeException e) {
            return null;
        }
    }

    static String field(String info, String name) {
        for (String line : info.split("\r\n")) {
            if (line.startsWith(name + ":")) return line.substring(name.length() + 1);
        }
        return null;
    }
}
