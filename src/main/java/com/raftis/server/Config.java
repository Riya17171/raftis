package com.raftis.server;

import com.raftis.persistence.FsyncPolicy;

import java.nio.file.Path;

/** Server settings, parsed from command-line flags. */
public record Config(String bind, int port, Path dir, boolean appendOnly, FsyncPolicy fsync) {

    public static final String USAGE = """
            usage: raftis [options]
              --port <n>                          TCP port (default 6380; 0 picks a free port)
              --bind <address>                    address to listen on (default 127.0.0.1)
              --dir <path>                        where data files are stored (default ./data)
              --appendonly yes|no                 log every write to an append-only file (default yes)
              --appendfsync always|everysec|no    when to fsync that log (default everysec)""";

    public static Config defaults() {
        return new Config("127.0.0.1", 6380, Path.of("data"), true, FsyncPolicy.EVERYSEC);
    }

    public static Config parse(String[] args) {
        Config c = defaults();
        for (int i = 0; i < args.length; i++) {
            String flag = args[i];
            if (i + 1 >= args.length) throw new IllegalArgumentException("missing value for " + flag);
            String value = args[++i];
            c = switch (flag) {
                case "--port" -> new Config(c.bind, parsePort(value), c.dir, c.appendOnly, c.fsync);
                case "--bind" -> new Config(value, c.port, c.dir, c.appendOnly, c.fsync);
                case "--dir" -> new Config(c.bind, c.port, Path.of(value), c.appendOnly, c.fsync);
                case "--appendonly" -> new Config(c.bind, c.port, c.dir, parseYesNo(flag, value), c.fsync);
                case "--appendfsync" -> new Config(c.bind, c.port, c.dir, c.appendOnly, FsyncPolicy.parse(value));
                default -> throw new IllegalArgumentException("unknown option " + flag);
            };
        }
        return c;
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
