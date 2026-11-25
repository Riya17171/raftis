package com.raftis.server;

import com.raftis.persistence.FsyncPolicy;
import com.raftis.raft.ClusterConfig;

import java.nio.file.Path;

/**
 * Server settings, parsed from command-line flags.
 *
 * @param snapshotEntries cluster mode: take a snapshot after this many log entries (0 = the default)
 * @param metricsPort     serve Prometheus metrics over HTTP on this port (0 = any free port, -1 = off)
 */
public record Config(String bind, int port, Path dir, boolean appendOnly, FsyncPolicy fsync, ClusterConfig cluster,
                     long snapshotEntries, int metricsPort) {

    /** A standalone (non-cluster) configuration. */
    public Config(String bind, int port, Path dir, boolean appendOnly, FsyncPolicy fsync) {
        this(bind, port, dir, appendOnly, fsync, null, 0, -1);
    }

    /** A cluster configuration with default Raft settings. */
    public Config(String bind, int port, Path dir, boolean appendOnly, FsyncPolicy fsync, ClusterConfig cluster) {
        this(bind, port, dir, appendOnly, fsync, cluster, 0, -1);
    }

    public Config withMetricsPort(int metricsPort) {
        return new Config(bind, port, dir, appendOnly, fsync, cluster, snapshotEntries, metricsPort);
    }

    public static final String USAGE = """
            usage: raftis [options]
              --port <n>                          TCP port (default 6380; 0 picks a free port)
              --bind <address>                    address to listen on (default 127.0.0.1)
              --dir <path>                        where data files are stored (default ./data)
              --appendonly yes|no                 log every write to an append-only file (default yes)
              --appendfsync always|everysec|no    when to fsync that log (default everysec)
              --metrics-port <n>                  serve Prometheus metrics at http://<bind>:<n>/metrics (default off)

            cluster mode (Raft replication):
              --node-id <n>                       this node's id
              --cluster <members>                 every node as id=host:clientPort:raftPort, comma-separated
                                                  e.g. 1=127.0.0.1:6380:7380,2=127.0.0.1:6381:7381,3=127.0.0.1:6382:7382
                                                  (the node's own entry sets its ports; --port is ignored)
              --snapshot-entries <n>              compact the Raft log every n entries (default 100000)""";

    public static Config defaults() {
        return new Config("127.0.0.1", 6380, Path.of("data"), true, FsyncPolicy.EVERYSEC);
    }

    public static Config parse(String[] args) {
        Config d = defaults();
        String bind = d.bind;
        int port = d.port;
        Path dir = d.dir;
        boolean appendOnly = d.appendOnly;
        FsyncPolicy fsync = d.fsync;
        long snapshotEntries = 0;
        int metricsPort = -1;
        Integer nodeId = null;
        String members = null;
        for (int i = 0; i < args.length; i++) {
            String flag = args[i];
            if (i + 1 >= args.length) throw new IllegalArgumentException("missing value for " + flag);
            String value = args[++i];
            switch (flag) {
                case "--port" -> port = parsePort(value);
                case "--bind" -> bind = value;
                case "--dir" -> dir = Path.of(value);
                case "--appendonly" -> appendOnly = parseYesNo(flag, value);
                case "--appendfsync" -> fsync = FsyncPolicy.parse(value);
                case "--node-id" -> nodeId = parseInt("node id", value);
                case "--cluster" -> members = value;
                case "--metrics-port" -> metricsPort = parsePort(value);
                case "--snapshot-entries" -> {
                    snapshotEntries = parseInt("--snapshot-entries", value);
                    if (snapshotEntries <= 0) throw new IllegalArgumentException("--snapshot-entries must be positive");
                }
                default -> throw new IllegalArgumentException("unknown option " + flag);
            }
        }
        if ((nodeId == null) != (members == null)) {
            throw new IllegalArgumentException("--node-id and --cluster must be used together");
        }
        ClusterConfig cluster = nodeId == null ? null : ClusterConfig.parse(nodeId, members);
        return new Config(bind, port, dir, appendOnly, fsync, cluster, snapshotEntries, metricsPort);
    }

    private static int parseInt(String what, String value) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid " + what + " " + value);
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
