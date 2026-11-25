package com.raftis;

import com.raftis.server.Config;
import com.raftis.server.Server;

import java.util.List;

public final class Main {
    private Main() {}

    public static void main(String[] args) throws Exception {
        // Look host names up again after a second, not the JVM default of 30 s (and 10 s for
        // failures): in Docker or Kubernetes a restarted peer can come back with a new IP address.
        java.security.Security.setProperty("networkaddress.cache.ttl", "1");
        java.security.Security.setProperty("networkaddress.cache.negative.ttl", "1");
        if (List.of(args).contains("--help") || List.of(args).contains("-h")) {
            System.out.println(Config.USAGE);
            return;
        }
        Config config;
        try {
            config = Config.parse(args);
        } catch (IllegalArgumentException e) {
            System.err.println("raftis: " + e.getMessage());
            System.err.println(Config.USAGE);
            System.exit(2);
            return;
        }
        Server server = new Server(config);
        server.start();
        // Ctrl+C / SIGTERM: stop the loop and flush data to disk before the JVM exits.
        Runtime.getRuntime().addShutdownHook(new Thread(server::shutdown, "raftis-shutdown"));
        server.run();
    }
}
