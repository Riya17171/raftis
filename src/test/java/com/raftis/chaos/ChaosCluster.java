package com.raftis.chaos;

import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * A cluster of real raftis server processes on this machine, with every Raft link running through
 * a {@link FaultyNetwork}, plus the controls a chaos test needs: kill, pause, resume, restart.
 */
final class ChaosCluster implements AutoCloseable {
    final int size;
    final FaultyNetwork network = new FaultyNetwork();
    private final Path dir;
    private final List<String> extraArgs;
    private final int[] clientPorts;
    private final int[] raftPorts;
    private final String[] members; // each node's own --cluster string (peers point at proxies)
    private final Process[] processes;
    private final boolean[] paused;

    ChaosCluster(int size, Path dir, List<String> extraArgs) throws IOException {
        this.size = size;
        this.dir = dir;
        this.extraArgs = extraArgs;
        clientPorts = new int[size + 1];
        raftPorts = new int[size + 1];
        members = new String[size + 1];
        processes = new Process[size + 1];
        paused = new boolean[size + 1];
        for (int id = 1; id <= size; id++) {
            clientPorts[id] = freePort();
            raftPorts[id] = freePort();
        }
        // Node `from` reaches node `to` through the proxy for that direction.
        for (int from = 1; from <= size; from++) {
            StringBuilder sb = new StringBuilder();
            for (int to = 1; to <= size; to++) {
                int raftPort = to == from ? raftPorts[to] : network.addLink(from, to, raftPorts[to]);
                if (sb.length() > 0) sb.append(',');
                sb.append(to).append("=127.0.0.1:").append(clientPorts[to]).append(':').append(raftPort);
            }
            members[from] = sb.toString();
        }
    }

    void startAll() throws IOException {
        for (int id = 1; id <= size; id++) start(id);
    }

    int clientPort(int id) {
        return clientPorts[id];
    }

    /** The node id that listens on this client port, or -1. */
    int nodeForPort(int port) {
        for (int id = 1; id <= size; id++) if (clientPorts[id] == port) return id;
        return -1;
    }

    boolean isUp(int id) {
        return processes[id] != null && processes[id].isAlive();
    }

    boolean isPaused(int id) {
        return paused[id];
    }

    void start(int id) throws IOException {
        if (isUp(id)) return;
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        String classpath = System.getProperty("raftis.serverClasspath", System.getProperty("java.class.path"));
        List<String> cmd = new ArrayList<>(List.of(java, "-Xmx256m", "-XX:+UseSerialGC", "-cp", classpath, "com.raftis.Main",
                "--node-id", String.valueOf(id), "--cluster", members[id], "--dir", dir.resolve("node" + id).toString()));
        cmd.addAll(extraArgs);
        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.redirectErrorStream(true);
        pb.redirectOutput(ProcessBuilder.Redirect.appendTo(dir.resolve("node" + id + ".log").toFile()));
        processes[id] = pb.start();
        paused[id] = false;
    }

    /** SIGKILL: like a crash or power loss (minus losing the OS page cache). */
    void kill(int id) throws InterruptedException {
        if (processes[id] == null) return;
        processes[id].destroyForcibly();
        processes[id].waitFor();
        processes[id] = null;
        paused[id] = false;
    }

    /** SIGSTOP: the process freezes mid-instruction, like a very long GC pause or a stalled VM. */
    void pause(int id) throws IOException, InterruptedException {
        if (!isUp(id) || paused[id]) return;
        signal("STOP", id);
        paused[id] = true;
    }

    /** SIGCONT: it carries on exactly where it stopped, unaware that time has passed. */
    void resume(int id) throws IOException, InterruptedException {
        if (!paused[id]) return;
        signal("CONT", id);
        paused[id] = false;
    }

    private void signal(String sig, int id) throws IOException, InterruptedException {
        new ProcessBuilder("kill", "-" + sig, String.valueOf(processes[id].pid())).inheritIO().start().waitFor();
    }

    /** Undoes every fault: heals the network, resumes paused nodes, restarts dead ones. */
    void healEverything() throws IOException, InterruptedException {
        network.healAll();
        for (int id = 1; id <= size; id++) {
            resume(id);
            start(id);
        }
    }

    /**
     * The node that claims to be leader with the highest term, or -1. (A leader cut off from the
     * others may still think it leads, in an older term; the real one has the highest term.)
     */
    int leader() {
        int best = -1;
        long bestTerm = -1;
        for (int id = 1; id <= size; id++) {
            if (!isUp(id) || paused[id]) continue;
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
        try (Jedis j = new Jedis("127.0.0.1", clientPorts[id], 300)) {
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

    @Override
    public void close() throws InterruptedException {
        for (int id = 1; id <= size; id++) kill(id);
        network.close();
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
