package com.raftis.playground;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A live, clickable raftis cluster in the browser. Starts real server processes with every Raft
 * link going through a {@link FaultyNetwork}, generates client traffic, polls each node's
 * {@code /metrics}, and serves a page where you can kill, freeze, restart and isolate nodes, cut
 * links, and watch elections, replication and catch-up happen.
 *
 * <pre>scripts/playground.sh            # then open http://localhost:8080</pre>
 */
public final class Playground {
    private static final long POLL_MS = 100;
    private static final DateTimeFormatter CLOCK = DateTimeFormatter.ofPattern("HH:mm:ss");

    /** One node as seen by the last poll. */
    record NodeView(int id, String status, String role, long term, long leaderId, long commit, long applied,
                    long lastLog, long snapshot, long elections, long snapshotsSent, long snapshotsInstalled,
                    long keys, double fsyncMs, double commitMs) {}

    private record Event(String time, String kind, String text) {}

    private final LocalCluster cluster;
    private final Workload workload;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofMillis(300)).build();
    private final ExecutorService actions = Executors.newSingleThreadExecutor();
    private final Deque<Event> events = new ArrayDeque<>();
    private final Deque<Double> opsHistory = new ArrayDeque<>();
    private final Map<Long, Long> lastBytes = new HashMap<>();
    private final Map<Long, Double> linkRates = new HashMap<>();
    private final double[] fsyncSum;
    private final long[] fsyncCount;
    private final double[] commitSum;
    private final long[] commitCount;
    private final double[] fsyncMs;
    private final double[] commitMs;

    private volatile String stateJson = "{}";
    private volatile boolean stopping;
    private HttpServer server;
    private volatile int leaderPort = -1;
    private int leader = -1;
    private long leaderTerm = -1;
    private long leaderLostAt = -1; // when the cluster last lost its leader, or -1
    private long lastOk;
    private long lastErrors;
    private long lastRateAt = System.nanoTime();
    private double opsPerSec;
    private double errorsPerSec;
    private long polls;

    Playground(LocalCluster cluster) {
        this.cluster = cluster;
        this.workload = new Workload(() -> leaderPort);
        int n = cluster.size() + 1;
        fsyncSum = new double[n];
        fsyncCount = new long[n];
        commitSum = new double[n];
        commitCount = new long[n];
        fsyncMs = new double[n];
        commitMs = new double[n];
    }

    public static void main(String[] args) throws Exception {
        int nodes = 5;
        int port = 8080;
        int firstClientPort = 6380;
        for (int i = 0; i + 1 < args.length; i += 2) {
            switch (args[i]) {
                case "--nodes" -> nodes = Integer.parseInt(args[i + 1]);
                case "--port" -> port = Integer.parseInt(args[i + 1]);
                case "--first-client-port" -> firstClientPort = Integer.parseInt(args[i + 1]);
                default -> {
                    System.err.println("usage: raftis-playground [--nodes 5] [--port 8080] [--first-client-port 6380]");
                    System.exit(2);
                }
            }
        }
        if (nodes < 1 || nodes > 9) throw new IllegalArgumentException("--nodes must be between 1 and 9");
        Path dir = Files.createTempDirectory("raftis-playground");
        LocalCluster cluster = new LocalCluster(nodes, dir, System.getProperty("java.class.path"), firstClientPort,
                List.of("--snapshot-entries", "1000"));
        Playground p = new Playground(cluster);
        Runtime.getRuntime().addShutdownHook(new Thread(p::stop, "playground-shutdown"));
        p.start(port);
        System.out.printf("%nraftis playground: %d nodes (client ports %d-%d), logs in %s%n", nodes,
                cluster.clientPort(1), cluster.clientPort(nodes), dir);
        System.out.printf("open http://localhost:%d  (Ctrl+C to stop)%n%n", port);
    }

    void start(int port) throws IOException {
        cluster.startAll();
        event("action", "started " + cluster.size() + " nodes; waiting for the first election");
        leaderLostAt = System.nanoTime();
        Thread.ofPlatform().daemon().name("poller").start(this::pollLoop);
        workload.start();
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), port), 0);
        server.createContext("/", this::page);
        server.createContext("/api/state", x -> send(x, 200, "application/json", stateJson));
        server.createContext("/api/", this::action);
        server.setExecutor(Executors.newFixedThreadPool(4));
        server.start();
    }

    void stop() {
        stopping = true;
        workload.stop();
        if (server != null) server.stop(0);
        try {
            cluster.close();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ------------------------------------------------------------------ polling

    private void pollLoop() {
        while (!stopping) {
            long started = System.nanoTime();
            try {
                poll();
            } catch (RuntimeException e) {
                event("alert", "playground error: " + e);
            }
            long sleep = POLL_MS - (System.nanoTime() - started) / 1_000_000;
            try {
                if (sleep > 0) Thread.sleep(sleep);
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    private void poll() {
        int n = cluster.size();
        List<CompletableFuture<Map<String, Double>>> fetches = new ArrayList<>();
        for (int id = 1; id <= n; id++) {
            if (!cluster.isUp(id) || cluster.isPaused(id)) {
                fetches.add(CompletableFuture.completedFuture(null));
                continue;
            }
            HttpRequest req = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + cluster.metricsPort(id) + "/metrics"))
                    .timeout(Duration.ofMillis(400)).build();
            fetches.add(http.sendAsync(req, HttpResponse.BodyHandlers.ofString())
                    .thenApply(r -> parseMetrics(r.body()))
                    .exceptionally(e -> null));
        }
        List<NodeView> views = new ArrayList<>();
        for (int id = 1; id <= n; id++) views.add(view(id, fetches.get(id - 1).join()));

        // The real leader is the one with the highest term: a cut-off old leader may still think it leads.
        NodeView best = views.stream().filter(v -> v.role.equals("leader")).max(Comparator.comparingLong(NodeView::term)).orElse(null);
        int newLeader = best == null ? -1 : best.id;
        if (newLeader != leader) {
            if (newLeader > 0) {
                String after = leaderLostAt > 0 ? String.format(" after %d ms without a leader", (System.nanoTime() - leaderLostAt) / 1_000_000) : "";
                event("leader", "node " + newLeader + " is the leader (term " + best.term + ")" + after);
                leaderLostAt = -1;
            } else if (leaderLostAt < 0) {
                leaderLostAt = System.nanoTime();
                event("alert", "no leader: election in progress");
            }
            leader = newLeader;
        }
        leaderTerm = best == null ? -1 : best.term;
        leaderPort = newLeader > 0 ? cluster.clientPort(newLeader) : -1;

        // Traffic rates, smoothed over about half a second.
        long now = System.nanoTime();
        double secs = (now - lastRateAt) / 1e9;
        if (secs >= 0.25) {
            long okNow = workload.ok();
            long errNow = workload.errors();
            opsPerSec = smooth(opsPerSec, (okNow - lastOk) / secs);
            errorsPerSec = smooth(errorsPerSec, (errNow - lastErrors) / secs);
            lastOk = okNow;
            lastErrors = errNow;
            for (int a = 1; a <= n; a++) {
                for (int b = 1; b <= n; b++) {
                    if (a == b) continue;
                    long k = linkKey(a, b);
                    long bytes = cluster.network().bytesDelivered(a, b);
                    long before = lastBytes.getOrDefault(k, bytes);
                    linkRates.put(k, smooth(linkRates.getOrDefault(k, 0.0), (bytes - before) / secs));
                    lastBytes.put(k, bytes);
                }
            }
            lastRateAt = now;
            if (polls++ % 2 == 0) {
                opsHistory.addLast(workload.isOn() ? opsPerSec : 0);
                while (opsHistory.size() > 120) opsHistory.removeFirst();
            }
        }
        stateJson = json(views);
    }

    private static double smooth(double previous, double sample) {
        return previous * 0.5 + sample * 0.5;
    }

    private NodeView view(int id, Map<String, Double> m) {
        if (!cluster.isUp(id)) return new NodeView(id, "down", "", 0, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        if (cluster.isPaused(id)) return new NodeView(id, "frozen", "", 0, -1, 0, 0, 0, 0, 0, 0, 0, 0, fsyncMs[id], commitMs[id]);
        if (m == null) return new NodeView(id, "starting", "", 0, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        String role = m.getOrDefault("raftis_raft_role{role=\"leader\"}", 0.0) == 1 ? "leader"
                : m.getOrDefault("raftis_raft_role{role=\"candidate\"}", 0.0) == 1 ? "candidate" : "follower";
        fsyncMs[id] = recentAverageMs(m, "raftis_raft_fsync_seconds", fsyncSum, fsyncCount, id, fsyncMs[id]);
        commitMs[id] = recentAverageMs(m, "raftis_write_commit_seconds", commitSum, commitCount, id, commitMs[id]);
        return new NodeView(id, "up", role, l(m, "raftis_raft_term"), l(m, "raftis_raft_leader_id"),
                l(m, "raftis_raft_commit_index"), l(m, "raftis_raft_last_applied"), l(m, "raftis_raft_last_log_index"),
                l(m, "raftis_raft_snapshot_index"), l(m, "raftis_raft_elections_started_total"),
                l(m, "raftis_raft_snapshots_sent_total"), l(m, "raftis_raft_snapshots_installed_total"),
                l(m, "raftis_keys"), fsyncMs[id], commitMs[id]);
    }

    /** Average of a histogram over the observations since the last poll (keeps the old value if none). */
    private static double recentAverageMs(Map<String, Double> m, String name, double[] sums, long[] counts, int id, double old) {
        double sum = m.getOrDefault(name + "_sum", 0.0);
        long count = m.getOrDefault(name + "_count", 0.0).longValue();
        if (count < counts[id]) { // the node restarted, so its counters started again from zero
            sums[id] = 0;
            counts[id] = 0;
        }
        double avg = old;
        if (count > counts[id]) {
            double sample = (sum - sums[id]) / (count - counts[id]) * 1000;
            avg = old == 0 ? sample : smooth(old, sample);
        }
        sums[id] = sum;
        counts[id] = count;
        return avg;
    }

    private static long l(Map<String, Double> m, String name) {
        return m.getOrDefault(name, 0.0).longValue();
    }

    static Map<String, Double> parseMetrics(String text) {
        Map<String, Double> m = new HashMap<>();
        for (String line : text.split("\n")) {
            if (line.isEmpty() || line.startsWith("#")) continue;
            int space = line.lastIndexOf(' ');
            try {
                m.put(line.substring(0, space), Double.parseDouble(line.substring(space + 1)));
            } catch (RuntimeException ignored) {
                // not a sample line
            }
        }
        return m;
    }

    // ------------------------------------------------------------------ actions

    private void action(HttpExchange x) throws IOException {
        if (!x.getRequestMethod().equals("POST")) {
            send(x, 405, "text/plain", "POST only\n");
            return;
        }
        String what = x.getRequestURI().getPath().substring("/api/".length());
        Map<String, String> q = query(x.getRequestURI().getRawQuery());
        try {
            actions.submit(() -> {
                perform(what, q);
                return null;
            }).get();
            send(x, 200, "application/json", stateJson);
        } catch (Exception e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            send(x, 400, "text/plain", cause.getMessage() + "\n");
        }
    }

    private void perform(String what, Map<String, String> q) throws Exception {
        int node = q.containsKey("node") ? Integer.parseInt(q.get("node")) : -1;
        if (node == 0) node = leader;
        if (q.containsKey("node") && (node < 1 || node > cluster.size())) throw new IllegalArgumentException("no leader to do that to");
        boolean isLeader = node == leader;
        String who = "node " + node + (isLeader ? " (the leader)" : "");
        switch (what) {
            case "kill" -> {
                cluster.kill(node);
                if (isLeader) leaderLostAt = System.nanoTime();
                event("action", "kill -9 " + who);
            }
            case "freeze" -> {
                cluster.pause(node);
                if (isLeader) leaderLostAt = System.nanoTime();
                event("action", "froze " + who + " with SIGSTOP");
            }
            case "restart" -> {
                if (cluster.isPaused(node)) {
                    cluster.resume(node);
                    event("action", "unfroze node " + node + ": it wakes up unaware that time passed");
                } else {
                    cluster.start(node);
                    event("action", "restarted node " + node + "; it recovers from disk and catches up");
                }
            }
            case "isolate" -> {
                int isolated = node;
                Set<Integer> rest = IntStream.rangeClosed(1, cluster.size()).filter(i -> i != isolated).boxed().collect(Collectors.toSet());
                cluster.network().partition(List.of(Set.of(node), rest));
                if (isLeader) leaderLostAt = System.nanoTime();
                event("action", "cut " + who + " off from the network" + (isLeader ? "; it still thinks it leads" : ""));
            }
            case "link" -> {
                int a = Integer.parseInt(q.get("from"));
                int b = Integer.parseInt(q.get("to"));
                boolean anyCut = cluster.network().isCut(a, b) || cluster.network().isCut(b, a);
                if (anyCut) {
                    cluster.network().heal(a, b);
                    cluster.network().heal(b, a);
                    event("action", "reconnected node " + a + " and node " + b);
                } else {
                    cluster.network().cut(a, b);
                    cluster.network().cut(b, a);
                    event("action", "cut the link between node " + a + " and node " + b);
                }
            }
            case "heal" -> {
                cluster.healEverything();
                event("action", "healed everything: network reconnected, all nodes running");
            }
            case "traffic" -> {
                workload.setOn(Boolean.parseBoolean(q.get("on")));
                event("action", "client traffic " + (workload.isOn() ? "on" : "off"));
            }
            default -> throw new IllegalArgumentException("unknown action " + what);
        }
    }

    private static Map<String, String> query(String raw) {
        Map<String, String> q = new HashMap<>();
        if (raw == null) return q;
        for (String part : raw.split("&")) {
            int eq = part.indexOf('=');
            if (eq > 0) q.put(part.substring(0, eq), part.substring(eq + 1));
        }
        return q;
    }

    private void event(String kind, String text) {
        synchronized (events) {
            events.addFirst(new Event(LocalTime.now().format(CLOCK), kind, text));
            while (events.size() > 60) events.removeLast();
        }
    }

    // ------------------------------------------------------------------ JSON and HTTP

    private String json(List<NodeView> views) {
        StringBuilder sb = new StringBuilder(4096);
        sb.append("{\"leader\":").append(leader).append(",\"term\":").append(leaderTerm).append(",\"nodes\":[");
        for (int i = 0; i < views.size(); i++) {
            NodeView v = views.get(i);
            if (i > 0) sb.append(',');
            sb.append(String.format(java.util.Locale.ROOT,
                    "{\"id\":%d,\"status\":\"%s\",\"role\":\"%s\",\"term\":%d,\"leaderId\":%d,\"commit\":%d,\"applied\":%d,"
                            + "\"lastLog\":%d,\"snapshot\":%d,\"elections\":%d,\"snapshotsSent\":%d,\"snapshotsInstalled\":%d,"
                            + "\"keys\":%d,\"fsyncMs\":%.2f,\"commitMs\":%.2f,\"clientPort\":%d}",
                    v.id, v.status, v.role, v.term, v.leaderId, v.commit, v.applied, v.lastLog, v.snapshot, v.elections,
                    v.snapshotsSent, v.snapshotsInstalled, v.keys, v.fsyncMs, v.commitMs, cluster.clientPort(v.id)));
        }
        sb.append("],\"links\":[");
        boolean first = true;
        for (int a = 1; a <= cluster.size(); a++) {
            for (int b = 1; b <= cluster.size(); b++) {
                if (a == b) continue;
                if (!first) sb.append(',');
                first = false;
                sb.append(String.format(java.util.Locale.ROOT, "{\"from\":%d,\"to\":%d,\"cut\":%b,\"rate\":%.0f}",
                        a, b, cluster.network().isCut(a, b), linkRates.getOrDefault(linkKey(a, b), 0.0)));
            }
        }
        sb.append(String.format(java.util.Locale.ROOT,
                "],\"traffic\":{\"on\":%b,\"opsPerSec\":%.0f,\"errorsPerSec\":%.0f,\"acked\":%d,\"checks\":%d,\"lost\":%d,\"lastError\":%s,\"history\":[",
                workload.isOn(), opsPerSec, errorsPerSec, workload.acked(), workload.checks(), workload.lost(),
                quote(workload.lastError())));
        sb.append(opsHistory.stream().map(d -> String.format(java.util.Locale.ROOT, "%.0f", d)).collect(Collectors.joining(",")));
        sb.append("]},\"events\":[");
        synchronized (events) {
            sb.append(events.stream().map(e -> "{\"time\":\"" + e.time + "\",\"kind\":\"" + e.kind + "\",\"text\":" + quote(e.text) + "}")
                    .collect(Collectors.joining(",")));
        }
        return sb.append("]}").toString();
    }

    private static String quote(String s) {
        StringBuilder sb = new StringBuilder("\"");
        for (char c : s.toCharArray()) {
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                default -> {
                    if (c < 0x20) sb.append(String.format("\\u%04x", (int) c));
                    else sb.append(c);
                }
            }
        }
        return sb.append('"').toString();
    }

    private static long linkKey(int a, int b) {
        return ((long) a << 32) | b;
    }

    private void page(HttpExchange x) throws IOException {
        if (!x.getRequestURI().getPath().equals("/")) {
            send(x, 404, "text/plain", "not found\n");
            return;
        }
        try (InputStream in = Playground.class.getResourceAsStream("/playground/index.html")) {
            if (in == null) {
                send(x, 500, "text/plain", "index.html missing from the build\n");
                return;
            }
            send(x, 200, "text/html; charset=utf-8", new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
    }

    private static void send(HttpExchange x, int status, String type, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        x.getResponseHeaders().set("Content-Type", type);
        x.getResponseHeaders().set("Cache-Control", "no-store");
        x.sendResponseHeaders(status, bytes.length);
        x.getResponseBody().write(bytes);
        x.close();
    }
}
