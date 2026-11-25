package com.raftis;

import com.raftis.server.Config;
import com.raftis.server.Server;

import java.util.List;

public final class Main {
    private Main() {}

    public static void main(String[] args) throws Exception {
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
