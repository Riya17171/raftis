package com.raftis.server;

import com.raftis.persistence.FsyncPolicy;
import com.raftis.raft.ClusterConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MetricsTest {
    @TempDir
    Path dir;

    private final HttpClient http = HttpClient.newHttpClient();

    @Test
    void standaloneServerServesPrometheusMetrics() throws Exception {
        Server server = new Server(new Config("127.0.0.1", 0, dir, false, FsyncPolicy.NO).withMetricsPort(0));
        server.start();
        server.runInBackground();
        try (Jedis j = new Jedis("127.0.0.1", server.port())) {
            j.set("a", "1");
            j.set("b", "2");
            j.set("c", "3");
            HttpResponse<String> r = get(server, "/metrics");
            assertEquals(200, r.statusCode());
            assertTrue(r.headers().firstValue("Content-Type").orElse("").startsWith("text/plain; version=0.0.4"));
            Map<String, Double> m = com.raftis.playground.PlaygroundAccess.parse(r.body());
            assertEquals(3.0, m.get("raftis_keys"));
            assertEquals(3.0, m.get("raftis_commands_processed_total"));
            assertEquals(1.0, m.get("raftis_info{version=\"" + Server.VERSION + "\",mode=\"standalone\"}"));
            assertTrue(r.body().contains("# TYPE raftis_keys gauge"));
            assertEquals(404, get(server, "/nope").statusCode());
            HttpRequest post = HttpRequest.newBuilder(uri(server, "/metrics")).POST(HttpRequest.BodyPublishers.noBody()).build();
            assertEquals(405, http.send(post, HttpResponse.BodyHandlers.ofString()).statusCode());
        } finally {
            server.shutdown();
        }
    }

    @Test
    void clusterNodeReportsRaftStateFsyncAndCommitLatency() throws Exception {
        int clientPort = ClusterIntegrationTest.freePort();
        int raftPort = ClusterIntegrationTest.freePort();
        ClusterConfig cluster = ClusterConfig.parse(1, "1=127.0.0.1:" + clientPort + ":" + raftPort);
        Server server = new Server(new Config("127.0.0.1", 0, dir, true, FsyncPolicy.ALWAYS, cluster).withMetricsPort(0));
        server.start();
        server.runInBackground();
        try (Jedis j = new Jedis("127.0.0.1", clientPort, 5000)) {
            // Even alone, a node waits one election timeout (150-300 ms) before it elects itself.
            long deadline = System.currentTimeMillis() + 5000;
            while (!get(server, "/metrics").body().contains("raftis_raft_role{role=\"leader\"} 1")) {
                assertTrue(System.currentTimeMillis() < deadline, "the node never became leader");
                Thread.sleep(20);
            }
            for (int i = 0; i < 10; i++) j.incr("n");
            Map<String, Double> m = com.raftis.playground.PlaygroundAccess.parse(get(server, "/metrics").body());
            assertEquals(1.0, m.get("raftis_raft_role{role=\"leader\"}"));
            assertEquals(0.0, m.get("raftis_raft_role{role=\"follower\"}"));
            assertEquals(1.0, m.get("raftis_raft_leader_id"));
            assertEquals(10.0, m.get("raftis_client_writes_total{result=\"committed\"}"));
            assertEquals(10.0, m.get("raftis_write_commit_seconds_count"));
            assertTrue(m.get("raftis_raft_fsync_seconds_count") >= 1, "every commit needs an fsync");
            assertTrue(m.get("raftis_raft_commit_index") >= 11, "10 writes plus the leader's no-op");
            // Histogram buckets are cumulative and end with +Inf = count.
            assertEquals(m.get("raftis_raft_fsync_seconds_count"), m.get("raftis_raft_fsync_seconds_bucket{le=\"+Inf\"}"));
        } finally {
            server.shutdown();
        }
    }

    private HttpResponse<String> get(Server server, String path) throws IOException, InterruptedException {
        return http.send(HttpRequest.newBuilder(uri(server, path)).build(), HttpResponse.BodyHandlers.ofString());
    }

    private static URI uri(Server server, String path) {
        return URI.create("http://127.0.0.1:" + server.metricsPort() + path);
    }
}
