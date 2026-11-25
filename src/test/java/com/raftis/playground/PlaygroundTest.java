package com.raftis.playground;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Drives the playground through its HTTP API, as the page does: kill the leader, watch a new one take over. */
class PlaygroundTest {
    @TempDir
    Path dir;

    private final HttpClient http = HttpClient.newHttpClient();
    private int port;

    @Test
    void killingTheLeaderFromThePageElectsANewOneAndLosesNoWrites() throws Exception {
        String classpath = System.getProperty("raftis.serverClasspath", System.getProperty("java.class.path"));
        Playground playground = new Playground(new LocalCluster(3, dir, classpath, 0, List.of()));
        try (ServerSocket s = new ServerSocket(0)) {
            port = s.getLocalPort();
        }
        playground.start(port);
        try {
            assertTrue(get("/").contains("<title>raftis playground</title>"));

            String state = await(st -> number(st, "leader") > 0 && number(st, "acked") > 20, "no leader, or no traffic getting through");
            int oldLeader = (int) number(state, "leader");

            HttpResponse<String> killed = post("/api/kill?node=0"); // 0 = whoever leads now
            assertEquals(200, killed.statusCode());
            long ackedBefore = number(get("/api/state"), "acked");

            state = await(st -> number(st, "leader") > 0 && number(st, "leader") != oldLeader
                    && number(st, "acked") > ackedBefore + 20, "no new leader took over the traffic");
            assertNotEquals(oldLeader, number(state, "leader"));
            assertTrue(state.contains("kill -9 node " + oldLeader + " (the leader)"), state);
            assertTrue(state.matches("(?s).*node \\d is the leader \\(term \\d+\\) after \\d+ ms without a leader.*"), state);
            assertEquals(0, number(state, "lost"), "the durability check found lost writes");
            assertTrue(number(state, "checks") > 0, "the durability check never ran");

            assertEquals(400, post("/api/explode").statusCode());
        } finally {
            playground.stop();
        }
    }

    private String await(Predicate<String> condition, String message) throws Exception {
        long deadline = System.currentTimeMillis() + 20_000;
        String state = "";
        while (System.currentTimeMillis() < deadline) {
            state = get("/api/state");
            if (state.contains("\"nodes\"") && condition.test(state)) return state;
            Thread.sleep(100);
        }
        throw new AssertionError(message + "\n" + state);
    }

    private static long number(String json, String field) {
        Matcher m = Pattern.compile("\"" + field + "\":(-?\\d+)").matcher(json);
        if (!m.find()) throw new AssertionError("no " + field + " in " + json);
        return Long.parseLong(m.group(1));
    }

    private String get(String path) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path)).build(),
                HttpResponse.BodyHandlers.ofString()).body();
    }

    private HttpResponse<String> post(String path) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                .POST(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofString());
    }
}
