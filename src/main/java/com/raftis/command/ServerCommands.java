package com.raftis.command;

import com.raftis.Log;
import com.raftis.persistence.Persistence;
import com.raftis.raft.RaftNode;
import com.raftis.server.Connection;
import com.raftis.server.ReadMode;
import com.raftis.server.Server;
import com.raftis.store.Glob;
import com.raftis.store.Keyspace;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.raftis.command.CommandSpec.LOCAL;
import static com.raftis.command.CommandSpec.PUBSUB;
import static com.raftis.command.CommandSpec.WRITE;

/** Connection and server commands: PING, INFO, CONFIG GET, SAVE, SHUTDOWN... */
final class ServerCommands {
    private ServerCommands() {}

    static void register(CommandTable t) {
        t.add("ping", -1, PUBSUB | LOCAL, ServerCommands::ping);
        t.add("echo", 2, LOCAL, (ctx, a) -> ctx.out.bulk(a.get(1)));
        t.add("quit", -1, PUBSUB | LOCAL, ServerCommands::quit);
        t.add("command", -1, LOCAL, ServerCommands::command);
        t.add("select", 2, LOCAL, ServerCommands::select);
        t.add("client", -2, LOCAL, ServerCommands::client);
        t.add("config", -2, LOCAL, ServerCommands::config);
        t.add("info", -1, LOCAL, ServerCommands::info);
        t.add("readmode", -1, LOCAL, ServerCommands::readMode);
        t.add("dbsize", 1, 0, (ctx, a) -> ctx.out.integer(ctx.db().size()));
        t.add("flushall", -1, WRITE, ServerCommands::flush);
        t.add("flushdb", -1, WRITE, ServerCommands::flush);
        t.add("save", 1, LOCAL, ServerCommands::save);
        t.add("lastsave", 1, LOCAL, (ctx, a) -> ctx.out.integer(ctx.server().persistence().lastSaveMillis() / 1000));
        t.add("shutdown", -1, LOCAL, (ctx, a) -> ctx.server().requestStop());
    }

    private static void ping(Context ctx, List<byte[]> a) {
        if (a.size() > 2) throw CommandException.wrongArgs("ping");
        Connection c = ctx.conn();
        if (c != null && c.isSubscribed()) {
            ctx.out.arrayHeader(2).bulk("pong").bulk(a.size() == 2 ? a.get(1) : new byte[0]);
        } else if (a.size() == 2) {
            ctx.out.bulk(a.get(1));
        } else {
            ctx.out.simple("PONG");
        }
    }

    private static void quit(Context ctx, List<byte[]> a) {
        ctx.out.simple("OK");
        ctx.conn().closeAfterReply();
    }

    /** redis-cli sends COMMAND DOCS when it connects. An empty array means "no docs", which it handles. */
    private static void command(Context ctx, List<byte[]> a) {
        if (a.size() >= 2 && Args.is(a.get(1), "count")) ctx.out.integer(ctx.server().commands().size());
        else ctx.out.arrayHeader(0);
    }

    /**
     * READMODE [CONSISTENT|LEADER|STALE]: how this connection's reads behave in cluster mode.
     * CONSISTENT (default) is linearizable via ReadIndex. LEADER reads the leader's memory without
     * checking it's still the leader. STALE reads any node, including followers that may lag.
     */
    private static void readMode(Context ctx, List<byte[]> a) {
        Connection c = ctx.conn();
        if (a.size() == 1) {
            ctx.out.simple(c.readMode().name());
            return;
        }
        if (a.size() > 2) throw CommandException.wrongArgs("readmode");
        try {
            c.setReadMode(ReadMode.valueOf(Args.upper(a.get(1))));
        } catch (IllegalArgumentException e) {
            throw new CommandException("ERR READMODE must be CONSISTENT, LEADER or STALE");
        }
        ctx.out.simple("OK");
    }

    private static void select(Context ctx, List<byte[]> a) {
        if (Args.toLong(a.get(1)) != 0) throw new CommandException("ERR DB index is out of range");
        ctx.out.simple("OK");
    }

    private static void client(Context ctx, List<byte[]> a) {
        Connection c = ctx.conn();
        switch (Args.upper(a.get(1))) {
            case "SETNAME" -> {
                if (a.size() != 3) throw CommandException.wrongArgs("client|setname");
                c.setName(Args.str(a.get(2)));
                ctx.out.simple("OK");
            }
            case "GETNAME" -> ctx.out.bulk(c.name());
            case "ID" -> ctx.out.integer(c.id());
            case "SETINFO" -> ctx.out.simple("OK"); // client libraries send their name/version on connect
            default -> throw new CommandException(
                    "ERR unknown subcommand '" + Args.printable(a.get(1)) + "'. Try CLIENT HELP.");
        }
    }

    /** Only CONFIG GET is supported. redis-benchmark uses it to show the server's persistence settings. */
    private static void config(Context ctx, List<byte[]> a) {
        String sub = Args.upper(a.get(1));
        if (sub.equals("RESETSTAT")) {
            ctx.out.simple("OK");
            return;
        }
        if (!sub.equals("GET")) throw new CommandException("ERR CONFIG " + sub + " is not supported by raftis");
        if (a.size() < 3) throw CommandException.wrongArgs("config|get");
        Map<String, String> params = ctx.server().configParams();
        Set<String> matched = new LinkedHashSet<>();
        for (int i = 2; i < a.size(); i++) {
            byte[] pattern = Args.bytes(Args.lower(a.get(i)));
            for (String name : params.keySet()) {
                if (Glob.match(pattern, Args.bytes(name))) matched.add(name);
            }
        }
        List<byte[]> out = new ArrayList<>();
        for (String name : matched) {
            out.add(Args.bytes(name));
            out.add(Args.bytes(params.get(name)));
        }
        ctx.out.bulkArray(out);
    }

    private static void info(Context ctx, List<byte[]> a) {
        Server s = ctx.server();
        Keyspace db = ctx.db();
        Persistence p = s.persistence();
        StringBuilder sb = new StringBuilder();
        section(sb, "Server");
        line(sb, "raftis_version", Server.VERSION);
        line(sb, "java_version", System.getProperty("java.version"));
        line(sb, "process_id", ProcessHandle.current().pid());
        line(sb, "tcp_port", s.port());
        line(sb, "uptime_in_seconds", s.uptimeSeconds());
        section(sb, "Clients");
        line(sb, "connected_clients", s.clientCount());
        line(sb, "pubsub_channels", s.pubsub().channelCount());
        section(sb, "Memory");
        Runtime rt = Runtime.getRuntime();
        line(sb, "jvm_heap_used_bytes", rt.totalMemory() - rt.freeMemory());
        line(sb, "jvm_heap_max_bytes", rt.maxMemory());
        RaftNode raft = s.raft();
        if (raft == null) {
            section(sb, "Persistence");
            line(sb, "aof_enabled", p.aofEnabled() ? 1 : 0);
            line(sb, "aof_fsync", p.fsyncPolicy().configName());
            line(sb, "aof_generation", p.generation());
            line(sb, "last_save_time", p.lastSaveMillis() / 1000);
        } else {
            section(sb, "Raft");
            line(sb, "node_id", raft.id());
            line(sb, "role", raft.role().name().toLowerCase(java.util.Locale.ROOT));
            line(sb, "term", raft.term());
            line(sb, "leader_id", raft.leaderId());
            line(sb, "leader_address", s.leaderAddress());
            line(sb, "commit_index", raft.commitIndex());
            line(sb, "last_applied", raft.lastApplied());
            line(sb, "last_log_index", raft.lastLogIndex());
            line(sb, "snapshot_index", raft.snapshotIndex());
            line(sb, "connected_peers", s.connectedPeers());
            if (raft.isLeader()) {
                raft.matchIndexes().entrySet().stream().sorted(Map.Entry.comparingByKey())
                        .forEach(e -> line(sb, "peer_" + e.getKey() + "_match_index", e.getValue()));
            }
        }
        section(sb, "Stats");
        line(sb, "total_connections_received", s.totalConnections());
        line(sb, "total_commands_processed", s.totalCommands());
        line(sb, "expired_keys", db.expiredKeys());
        section(sb, "Keyspace");
        if (db.size() > 0) line(sb, "db0", "keys=" + db.size() + ",expires=" + db.expiresCount());
        ctx.out.bulk(sb.toString());
    }

    private static void section(StringBuilder sb, String name) {
        if (!sb.isEmpty()) sb.append("\r\n");
        sb.append("# ").append(name).append("\r\n");
    }

    private static void line(StringBuilder sb, String key, Object value) {
        sb.append(key).append(':').append(value).append("\r\n");
    }

    private static void flush(Context ctx, List<byte[]> a) {
        ctx.db().clear();
        ctx.dirty();
        ctx.out.simple("OK");
    }

    private static void save(Context ctx, List<byte[]> a) {
        if (ctx.server().raft() != null) {
            ctx.server().raft().compactNow(); // cluster mode: snapshot the state machine and compact the Raft log
            ctx.out.simple("OK");
            return;
        }
        try {
            ctx.server().persistence().save(ctx.db());
            ctx.out.simple("OK");
        } catch (IOException e) {
            Log.warn("SAVE failed: %s", e);
            throw new CommandException("ERR snapshot failed: " + e.getMessage());
        }
    }
}
