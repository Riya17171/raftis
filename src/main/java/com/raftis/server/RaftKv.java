package com.raftis.server;

import com.raftis.command.CommandSpec;
import com.raftis.metrics.Histogram;
import com.raftis.persistence.Snapshot;
import com.raftis.protocol.RespParser;
import com.raftis.protocol.RespWriter;
import com.raftis.raft.ClusterConfig;
import com.raftis.raft.RaftNode;
import com.raftis.raft.StateMachine;
import com.raftis.store.ByteKey;
import com.raftis.store.Keyspace;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

/**
 * Connects Raft to the Redis commands. It is both:
 * <ul>
 *   <li>the client side: turns client writes into Raft proposals and reads into ReadIndex
 *       requests, and holds each client until its answer is known;</li>
 *   <li>the {@link StateMachine}: runs committed commands against the keyspace on every node,
 *       and sends the reply to the waiting client on the node that proposed it.</li>
 * </ul>
 *
 * <p>A log entry is {@code [leader timestamp: 8 bytes][command in RESP]}. Every node applies
 * the command with "now" set to that timestamp, so TTLs and expiry come out identical
 * everywhere, whatever each machine's clock says.
 */
final class RaftKv implements StateMachine {
    static final String UNCERTAIN = "UNCERTAIN leadership changed before this write was confirmed; it may or may not have been applied";
    private static final String REPLACED = "ERR write was not applied: leadership changed before it was committed; safe to retry";
    private static final byte[] EXPIRE_COMMAND = "RAFTIS.EXPIRE".getBytes(StandardCharsets.US_ASCII);

    private record PendingWrite(long term, Connection conn, long proposedAt) {}

    private final Server server;
    private final Keyspace db;
    private final ClusterConfig cluster;
    private final TreeMap<Long, PendingWrite> pending = new TreeMap<>(); // log index -> waiting client
    private final ArrayDeque<Connection> resumable = new ArrayDeque<>();
    private final RespWriter sink = new RespWriter(); // replies nobody is waiting for (applied on followers)
    private RaftNode raft;
    private long expireProposal = -1;

    // Metrics, read by Server for /metrics.
    final Histogram commitLatency = new Histogram(); // proposal to applied, for client writes
    long writesCommitted;
    long writesUncertain;
    long writesReplaced;

    RaftKv(Server server, Keyspace db, ClusterConfig cluster) {
        this.server = server;
        this.db = db;
        this.cluster = cluster;
    }

    void attach(RaftNode raft) {
        this.raft = raft;
    }

    // ------------------------------------------------------------ client side

    void submitWrite(Connection c, List<byte[]> args) {
        if (!raft.isLeader()) {
            c.out.error(notLeaderError());
            return;
        }
        long index = raft.propose(encode(System.currentTimeMillis(), args));
        pending.put(index, new PendingWrite(raft.term(), c, System.nanoTime()));
        c.inflightWrites++;
    }

    void submitRead(Connection c, CommandSpec cmd, List<byte[]> args) {
        switch (c.readMode()) {
            case STALE -> server.executeRead(cmd, c, args);
            case LEADER -> {
                if (raft.isLeader()) server.executeRead(cmd, c, args);
                else c.out.error(notLeaderError());
            }
            case CONSISTENT -> {
                if (!raft.isLeader()) {
                    c.out.error(notLeaderError());
                    return;
                }
                c.blocked = true;
                raft.readIndex(new RaftNode.ReadCallback() {
                    @Override
                    public void ready() {
                        if (c.isOpen()) server.executeRead(cmd, c, args);
                        resume(c);
                    }

                    @Override
                    public void failed(String reason) {
                        if (c.isOpen()) c.out.error(notLeaderError());
                        resume(c);
                    }
                });
            }
        }
    }

    /** Leader only: delete keys whose TTL has passed, through the log so every node does the same. */
    void expireKeys() {
        if (!raft.isLeader() || expireProposal >= 0) return;
        List<ByteKey> keys = db.sampleExpiredKeys(1000);
        if (keys.isEmpty()) return;
        List<byte[]> args = new ArrayList<>(keys.size() + 1);
        args.add(EXPIRE_COMMAND);
        for (ByteKey k : keys) args.add(k.bytes());
        expireProposal = raft.propose(encode(System.currentTimeMillis(), args));
    }

    /** After losing leadership, writes still waiting may or may not commit. Tell those clients. */
    void checkLeadership() {
        if (raft.isLeader()) return;
        expireProposal = -1;
        failAllPending(UNCERTAIN);
    }

    /** Continues clients whose earlier command has finished. Returns true if there were any. */
    boolean resumeReady() {
        boolean any = !resumable.isEmpty();
        while (!resumable.isEmpty()) {
            Connection c = resumable.poll();
            if (c.isOpen() && !c.blocked) server.resume(c);
        }
        return any;
    }

    boolean hasResumable() {
        return !resumable.isEmpty();
    }

    String leaderAddress() {
        int leader = raft.leaderId();
        return leader < 0 ? "" : cluster.members().get(leader).clientAddress();
    }

    private String notLeaderError() {
        int leader = raft.leaderId();
        if (leader < 0 || leader == cluster.selfId()) return "CLUSTERDOWN no leader right now (election in progress); try again shortly";
        return "NOTLEADER " + leaderAddress();
    }

    // ------------------------------------------------------------ state machine side

    @Override
    public void apply(long index, long term, byte[] command) {
        PendingWrite waiting = pending.remove(index);
        if (waiting != null && waiting.term() != term) {
            // A different entry ended up at this index, so our client's command can never apply.
            failWrite(waiting, REPLACED);
            writesReplaced++;
            waiting = null;
        }
        if (waiting != null) {
            commitLatency.observeNanos(System.nanoTime() - waiting.proposedAt());
            writesCommitted++;
        }
        if (index == expireProposal) expireProposal = -1;
        if (command == null) return; // a leader's no-op

        long timestamp = ByteBuffer.wrap(command, 0, 8).getLong();
        List<byte[]> args = RespParser.parse(command, 8, command.length).args();
        Connection client = waiting != null && waiting.conn().isOpen() ? waiting.conn() : null;
        RespWriter out = client != null ? client.out : sink;
        db.setFixedTime(timestamp);
        try {
            server.executeReplicated(args, client, out);
        } finally {
            db.clearFixedTime();
            sink.reset();
        }
        if (waiting != null) writeDone(waiting.conn());
    }

    @Override
    public byte[] snapshot() {
        return Snapshot.toBytes(db);
    }

    @Override
    public void restore(byte[] snapshot) {
        db.clear();
        Snapshot.fromBytes(snapshot, db);
        failAllPending(UNCERTAIN); // their entries may be inside the snapshot; we can't tell which
    }

    // ------------------------------------------------------------ helpers

    private void failAllPending(String error) {
        if (pending.isEmpty()) return;
        List<PendingWrite> all = new ArrayList<>(pending.values()); // in log order, so replies stay ordered
        pending.clear();
        for (PendingWrite p : all) failWrite(p, error);
        writesUncertain += all.size();
    }

    private void failWrite(PendingWrite p, String error) {
        if (p.conn().isOpen()) p.conn().out.error(error);
        writeDone(p.conn());
    }

    private void writeDone(Connection c) {
        c.inflightWrites--;
        server.queueWrite(c);
        if (c.inflightWrites == 0 && c.blocked && c.deferred != null) resume(c);
    }

    private void resume(Connection c) {
        c.blocked = false;
        if (c.isOpen()) {
            resumable.add(c);
            server.queueWrite(c);
        }
    }

    static byte[] encode(long timestamp, List<byte[]> args) {
        RespWriter w = new RespWriter();
        w.command(args);
        return ByteBuffer.allocate(8 + w.length()).putLong(timestamp).put(w.buffer(), 0, w.length()).array();
    }
}
