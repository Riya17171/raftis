package com.raftis.playground;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Runs a raftis cluster as real server processes on this machine, with every Raft link going
 * through a {@link FaultyNetwork}, and the controls to break it: kill, pause, resume, restart.
 * Used by the playground and by the chaos test.
 */
public class LocalCluster implements AutoCloseable {
    private final int size;
    private final FaultyNetwork network = new FaultyNetwork();
    private final Path dir;
    private final String classpath;
    private final List<String> extraArgs;
    private final int[] clientPorts;
    private final int[] raftPorts;
    private final int[] metricsPorts;
    private final String[] members; // each node's own --cluster string: peers point at proxies
    private final Process[] processes;
    private final boolean[] paused;

    /**
     * @param firstClientPort node 1 serves clients on this port, node 2 on the next... (0 = any free ports)
     * @param extraArgs       more server options for every node, e.g. {@code --snapshot-entries 200}
     */
    public LocalCluster(int size, Path dir, String classpath, int firstClientPort, List<String> extraArgs) throws IOException {
        this.size = size;
        this.dir = dir;
        this.classpath = classpath;
        this.extraArgs = extraArgs;
        clientPorts = new int[size + 1];
        raftPorts = new int[size + 1];
        metricsPorts = new int[size + 1];
        members = new String[size + 1];
        processes = new Process[size + 1];
        paused = new boolean[size + 1];
        Files.createDirectories(dir);
        for (int id = 1; id <= size; id++) {
            clientPorts[id] = firstClientPort > 0 ? firstClientPort + id - 1 : freePort();
            raftPorts[id] = freePort();
            metricsPorts[id] = freePort();
        }
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

    public int size() {
        return size;
    }

    public FaultyNetwork network() {
        return network;
    }

    public int clientPort(int id) {
        return clientPorts[id];
    }

    public int metricsPort(int id) {
        return metricsPorts[id];
    }

    public Path logFile(int id) {
        return dir.resolve("node" + id + ".log");
    }

    /** The node id that serves clients on this port, or -1. */
    public int nodeForPort(int port) {
        for (int id = 1; id <= size; id++) if (clientPorts[id] == port) return id;
        return -1;
    }

    public synchronized boolean isUp(int id) {
        return processes[id] != null && processes[id].isAlive();
    }

    public synchronized boolean isPaused(int id) {
        return paused[id];
    }

    public void startAll() throws IOException {
        for (int id = 1; id <= size; id++) start(id);
    }

    public synchronized void start(int id) throws IOException {
        if (isUp(id)) return;
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        List<String> cmd = new ArrayList<>(List.of(java, "-Xmx256m", "-XX:+UseSerialGC", "-cp", classpath, "com.raftis.Main",
                "--node-id", String.valueOf(id), "--cluster", members[id], "--dir", dir.resolve("node" + id).toString(),
                "--metrics-port", String.valueOf(metricsPorts[id])));
        cmd.addAll(extraArgs);
        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.redirectErrorStream(true);
        pb.redirectOutput(ProcessBuilder.Redirect.appendTo(logFile(id).toFile()));
        processes[id] = pb.start();
        paused[id] = false;
    }

    /** SIGKILL: like a crash or power loss (minus losing the OS page cache). */
    public synchronized void kill(int id) throws InterruptedException {
        if (processes[id] == null) return;
        processes[id].destroyForcibly();
        processes[id].waitFor();
        processes[id] = null;
        paused[id] = false;
    }

    /** SIGSTOP: the process freezes mid-instruction, like a very long GC pause or a stalled VM. */
    public synchronized void pause(int id) throws IOException, InterruptedException {
        if (!isUp(id) || paused[id]) return;
        signal("STOP", id);
        paused[id] = true;
    }

    /** SIGCONT: it carries on exactly where it stopped, unaware that time has passed. */
    public synchronized void resume(int id) throws IOException, InterruptedException {
        if (!paused[id]) return;
        signal("CONT", id);
        paused[id] = false;
    }

    private void signal(String sig, int id) throws IOException, InterruptedException {
        new ProcessBuilder("kill", "-" + sig, String.valueOf(processes[id].pid())).inheritIO().start().waitFor();
    }

    /** Undoes every fault: heals the network, resumes paused nodes, restarts dead ones. */
    public synchronized void healEverything() throws IOException, InterruptedException {
        network.healAll();
        for (int id = 1; id <= size; id++) {
            resume(id);
            start(id);
        }
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
