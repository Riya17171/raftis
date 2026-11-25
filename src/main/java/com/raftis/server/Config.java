package com.raftis.server;

import com.raftis.persistence.FsyncPolicy;
import com.raftis.raft.ClusterConfig;

import java.nio.file.Path;

/** Server settings, parsed from command-line flags. */
public record Config(String bind, int port, Path dir, boolean appendOnly, FsyncPolicy fsync, ClusterConfig cluster) {

    /** A standalone (non-cluster) configuration. */
    public Config(String bind, int port, Path dir, boolean appendOnly, FsyncPolicy fsync) {
        this(bind, port, dir, appendOnly, fsync, null);
    }

    public static final String USAGE = """
            usage: raftis [options]
              --port <n>                          TCP port (default 6380; 0 picks a free port)
              --bind <address>                    address to listen on (default 127.0.0.1)
              --dir <path>                        where data files are stored (default ./data)
              --appendonly yes|no                 log every write to an append-only file (default yes)
              --appendfsync always|everysec|no    when to fsync that log (default everysec)

            cluster mode (Raft replication):
              --node-id <n>                       this node's id
              --cluster <members>                 every node as id=host:clientPort:raftPort, comma-separated
                                                  e.g. 1=127.0.0.1:6380:7380,2=127.0.0.1:6381:7381,3=127.0.0.1:6382:7382
                                                  (the node's own entry sets its ports; --port is ignored)""";

    public static Config defaults() {
        return new Config("127.0.0.1", 6380, Path.of("data"), true, FsyncPolicy.EVERYSEC);
    }

    public static Config parse(String[] args) {
        Config c = defaults();
        Integer nodeId = null;
        String members = null;
        for (int i = 0; i < args.length; i++) {
            String flag = args[i];
            if (i + 1 >= args.length) throw new IllegalArgumentException("missing value for " + flag);
            String value = args[++i];
            switch (flag) {
                case "--port" -> c = new Config(c.bind, parsePort(value), c.dir, c.appendOnly, c.fsync);
                case "--bind" -> c = new Config(value, c.port, c.dir, c.appendOnly, c.fsync);
                case "--dir" -> c = new Config(c.bind, c.port, Path.of(value), c.appendOnly, c.fsync);
                case "--appendonly" -> c = new Config(c.bind, c.port, c.dir, parseYesNo(flag, value), c.fsync);
                case "--appendfsync" -> c = new Config(c.bind, c.port, c.dir, c.appendOnly, FsyncPolicy.parse(value));
                case "--node-id" -> nodeId = parseNodeId(value);
                case "--cluster" -> members = value;
                default -> throw new IllegalArgumentException("unknown option " + flag);
            }
        }
        if ((nodeId == null) != (members == null)) {
            throw new IllegalArgumentException("--node-id and --cluster must be used together");
        }
        if (nodeId == null) return c;
        return new Config(c.bind, c.port, c.dir, c.appendOnly, c.fsync, ClusterConfig.parse(nodeId, members));
    }

    private static int parseNodeId(String value) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid node id " + value);
        }
    }

    private static int parsePort(String value) {
        try {
            int port = Integer.parseInt(value);
            if (port < 0 || port > 65535) throw new NumberFormatException();
            return port;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid port " + value);
        }
    }

    private static boolean parseYesNo(String flag, String value) {
        return switch (value) {
            case "yes" -> true;
            case "no" -> false;
            default -> throw new IllegalArgumentException(flag + " must be yes or no");
        };
    }
}
