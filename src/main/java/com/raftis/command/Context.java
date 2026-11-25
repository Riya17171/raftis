package com.raftis.command;

import com.raftis.protocol.RespWriter;
import com.raftis.server.Connection;
import com.raftis.server.Server;
import com.raftis.store.Keyspace;

import java.util.List;

/**
 * Everything a command handler needs: the dataset, where to write the reply, and a way to
 * record what it changed.
 *
 * <p><b>Propagation.</b> When a write command changes data, the command is logged to the
 * append-only file so it can be replayed after a restart. Some commands must be logged in a
 * different form than they were sent. For example, {@code EXPIRE k 10} is logged as
 * {@code PEXPIREAT k <absolute ms>}; otherwise replaying the log tomorrow would give the key a
 * fresh 10 seconds. Handlers call {@link #propagateAs} for those cases.
 */
public final class Context {
    private final Server server;
    private final Connection conn;
    public final RespWriter out;
    private boolean dirty;
    private List<byte[]> propagateAs;

    public Context(Server server, Connection conn, RespWriter out) {
        this.server = server;
        this.conn = conn;
        this.out = out;
    }

    public Keyspace db() {
        return server.db();
    }

    public Server server() {
        return server;
    }

    /** The client that sent the command, or null while replaying the append-only file. */
    public Connection conn() {
        return conn;
    }

    /** Marks the dataset as changed, so the command gets logged. */
    public void dirty() {
        dirty = true;
    }

    /** Marks the dataset as changed and logs {@code args} instead of the original command. */
    public void propagateAs(List<byte[]> args) {
        dirty = true;
        propagateAs = args;
    }

    public boolean isDirty() {
        return dirty;
    }

    /** What to write to the log: the rewritten command if there is one, otherwise the original. */
    public List<byte[]> propagation(List<byte[]> original) {
        return propagateAs != null ? propagateAs : original;
    }
}
