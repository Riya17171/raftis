package com.raftis.server;

import com.raftis.Log;
import com.raftis.command.Args;
import com.raftis.command.CommandException;
import com.raftis.command.CommandSpec;
import com.raftis.command.CommandTable;
import com.raftis.command.Context;
import com.raftis.persistence.FsyncPolicy;
import com.raftis.persistence.Persistence;
import com.raftis.protocol.ProtocolException;
import com.raftis.protocol.RespParser;
import com.raftis.protocol.RespWriter;
import com.raftis.raft.ClusterConfig;
import com.raftis.raft.FileRaftStorage;
import com.raftis.raft.Message;
import com.raftis.raft.PeerTransport;
import com.raftis.raft.RaftConfig;
import com.raftis.raft.RaftNode;
import com.raftis.store.Keyspace;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * The server: a single-threaded event loop built on Java NIO, like Redis's.
 *
 * <p>One thread does everything: accept connections, read and parse commands, run them, and
 * write replies. Because only this thread touches the data, there are no locks and every
 * command is atomic. It's fast because commands are in-memory operations that take
 * microseconds; the slow part, waiting on the network, is handled by the {@link Selector}
 * telling us which sockets are ready, so the thread never blocks on one client.
 *
 * <p>Each pass of the loop:
 * <ol>
 *   <li>wait (up to 100 ms) for sockets to become readable/writable</li>
 *   <li>read, parse and run every complete command, queueing replies</li>
 *   <li>run timers (active key expiry every 100 ms)</li>
 *   <li><b>before sending any reply</b>, write this pass's changes to the append-only file
 *       (and fsync in {@code always} mode). One fsync covers every write in the pass: that's
 *       group commit. A client never gets "OK" for data that isn't on disk yet.</li>
 *   <li>send the queued replies</li>
 * </ol>
 */
public final class Server {
    public static final String VERSION = "0.2.0";
    private static final long EXPIRE_CYCLE_MS = 100;
    private static final long RAFT_TICK_MS = 10;
    private static final int PUBSUB_OUTPUT_LIMIT = 32 * 1024 * 1024; // like Redis's pubsub buffer limit

    private final Config config;
    private final Keyspace db = new Keyspace(System::currentTimeMillis);
    private final CommandTable commands = CommandTable.standard();
    private final PubSub pubsub = new PubSub(this::queueWrite);
    private final Persistence persistence;
    private final Set<Connection> clients = new HashSet<>();
    private final Set<Connection> pendingWrites = new LinkedHashSet<>();
    private final ConcurrentLinkedQueue<Runnable> tasks = new ConcurrentLinkedQueue<>();
    private final CountDownLatch stopped = new CountDownLatch(1);
    private final RespWriter replaySink = new RespWriter();
    private final long startedAt = System.currentTimeMillis();

    private Selector selector;
    private ServerSocketChannel listener;
    private int port;
    private volatile boolean running;
    private long nextExpireCycle;
    private long nextClientId;
    private long totalCommands;
    private long totalConnections;

    // Cluster mode only (null when running as a single standalone server).
    private RaftNode raft;
    private RaftKv raftKv;
    private FileRaftStorage raftStorage;
    private PeerTransport transport;
    private long nextTick;

    public Server(Config config) {
        this.config = config;
        this.persistence = new Persistence(config.dir(), config.appendOnly(), config.fsync());
    }

    /** Binds the port and loads data from disk. Call {@link #run()} afterwards. */
    public void start() throws IOException {
        try {
            selector = Selector.open();
            listener = ServerSocketChannel.open();
            ClusterConfig cluster = config.cluster();
            int clientPort = cluster != null ? cluster.self().clientPort() : config.port();
            listener.bind(new InetSocketAddress(config.bind(), clientPort), 511);
            listener.configureBlocking(false);
            listener.register(selector, SelectionKey.OP_ACCEPT);
            port = ((InetSocketAddress) listener.getLocalAddress()).getPort();

            long t0 = System.nanoTime();
            if (cluster == null) {
                persistence.load(db, this::replay);
            } else {
                // The Raft log replaces the append-only file: it is the durable record of every write.
                // --appendfsync no turns off fsync for benchmarking only: it breaks durability on power loss.
                boolean fsync = config.fsync() != FsyncPolicy.NO;
                if (!fsync) Log.warn("fsync is OFF: acknowledged writes can be lost on power failure (benchmarking only)");
                raftStorage = FileRaftStorage.open(config.dir(), fsync);
                raftKv = new RaftKv(this, db, cluster);
                RaftConfig raftConfig = RaftConfig.defaults();
                if (config.snapshotEntries() > 0) raftConfig = raftConfig.withSnapshotThreshold(config.snapshotEntries());
                raft = new RaftNode(cluster.selfId(), cluster.peerIds(), raftConfig, raftStorage, raftKv, new Random());
                raftKv.attach(raft);
                transport = new PeerTransport(selector, cluster.peerAddresses(), raft::step);
                transport.bind(new InetSocketAddress(config.bind(), cluster.self().raftPort()));
                Log.info("cluster mode: node %d of %d, Raft port %d, term %d, log up to index %d",
                        cluster.selfId(), cluster.members().size(), cluster.self().raftPort(), raft.term(), raft.lastLogIndex());
            }
            Log.info("DB loaded from disk: %d keys in %.3f seconds", db.size(), (System.nanoTime() - t0) / 1e9);
        } catch (IOException | RuntimeException e) {
            closeQuietly();
            throw e;
        }
        running = true;
        Log.info("raftis %s ready to accept connections on %s:%d", VERSION, config.bind(), port);
    }

    /** Runs the event loop on the calling thread until {@link #shutdown()} or SHUTDOWN. */
    public void run() {
        try {
            loop();
        } catch (Exception e) {
            Log.warn("event loop stopped because of an error: %s", e);
        } finally {
            closeEverything();
            stopped.countDown();
        }
    }

    public Thread runInBackground() {
        Thread t = new Thread(this::run, "raftis-event-loop");
        t.start();
        return t;
    }

    /** Stops the loop from another thread and waits until data is safely flushed to disk. */
    public void shutdown() {
        if (stopped.getCount() == 0) return;
        running = false;
        Selector s = selector;
        if (s != null) s.wakeup();
        try {
            if (!stopped.await(10, TimeUnit.SECONDS)) Log.warn("timed out waiting for the event loop to stop");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Called on the loop thread by the SHUTDOWN command. */
    public void requestStop() {
        Log.info("user requested shutdown...");
        running = false;
    }

    /** Runs a task on the event-loop thread. Safe to call from any thread. */
    public void execute(Runnable task) {
        tasks.add(task);
        selector.wakeup();
    }

    private void loop() throws IOException {
        while (running) {
            long nextTimer = raft == null ? nextExpireCycle : Math.min(nextExpireCycle, nextTick);
            long wait = Math.max(1, nextTimer - System.currentTimeMillis());
            if (raftKv != null && raftKv.hasResumable()) selector.selectNow();
            else selector.select(wait);
            Set<SelectionKey> ready = selector.selectedKeys();
            for (SelectionKey key : ready) {
                if (!key.isValid()) continue;
                if (key.attachment() instanceof PeerTransport.Endpoint) {
                    transport.onReady(key); // Raft traffic between nodes
                    continue;
                }
                if (key.isAcceptable()) {
                    accept();
                    continue;
                }
                Connection c = (Connection) key.attachment();
                if (key.isReadable()) onReadable(c);
                if (key.isValid() && key.isWritable()) flush(c);
            }
            ready.clear();

            Runnable task;
            while ((task = tasks.poll()) != null) task.run();

            long now = System.currentTimeMillis();
            if (raft != null && now >= nextTick) {
                raft.tick();
                nextTick = now + RAFT_TICK_MS;
            }
            if (now >= nextExpireCycle) {
                if (raft == null) db.activeExpireCycle();
                else raftKv.expireKeys(); // cluster mode: only the leader expires keys, through the log
                nextExpireCycle = now + EXPIRE_CYCLE_MS;
            }
            beforeSleep();
        }
    }

    private void accept() throws IOException {
        SocketChannel channel;
        while ((channel = listener.accept()) != null) {
            channel.configureBlocking(false);
            channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
            Connection c = new Connection(++nextClientId, channel);
            c.key = channel.register(selector, SelectionKey.OP_READ, c);
            clients.add(c);
            totalConnections++;
        }
    }

    private void onReadable(Connection c) {
        int n;
        try {
            n = c.readFrom();
        } catch (IOException e) {
            close(c);
            return;
        }
        if (n < 0) {
            close(c);
            return;
        }
        processInput(c);
    }

    /** Parses and runs every complete command in the input buffer (a client may pipeline many). */
    void processInput(Connection c) {
        while (!c.shouldCloseAfterReply() && !c.blocked) {
            RespParser.Result parsed;
            try {
                parsed = RespParser.parse(c.in, c.inStart, c.inEnd);
            } catch (ProtocolException e) {
                c.out.error("ERR Protocol error: " + e.getMessage());
                c.closeAfterReply();
                break;
            }
            if (parsed == null) break; // incomplete: wait for more bytes
            c.inStart = parsed.next();
            if (!parsed.args().isEmpty()) execute(c, parsed.args());
        }
        c.afterParse();
        if (c.pendingOutput() > 0 || c.shouldCloseAfterReply()) queueWrite(c);
    }

    private void execute(Connection c, List<byte[]> args) {
        CommandSpec cmd = commands.lookup(args.get(0));
        if (raftKv != null && c.inflightWrites > 0) {
            // Earlier writes from this client are still in flight. More writes can follow them into
            // the log (their replies will come back in order); anything else must wait its turn.
            boolean canPipeline = cmd != null && cmd.has(CommandSpec.WRITE) && cmd.arityMatches(args.size()) && raft.isLeader();
            if (!canPipeline) {
                c.deferred = args;
                c.blocked = true;
                return;
            }
        }
        totalCommands++;
        if (cmd == null) {
            c.out.error("ERR unknown command '" + Args.printable(args.get(0)) + "'");
            return;
        }
        if (!cmd.arityMatches(args.size())) {
            c.out.error("ERR wrong number of arguments for '" + cmd.name() + "' command");
            return;
        }
        if (c.isSubscribed() && !cmd.has(CommandSpec.PUBSUB)) {
            c.out.error("ERR Can't execute '" + cmd.name()
                    + "': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context");
            return;
        }
        if (raftKv != null && !cmd.has(CommandSpec.LOCAL)) {
            if (cmd.has(CommandSpec.WRITE)) raftKv.submitWrite(c, args);
            else raftKv.submitRead(c, cmd, args);
            return;
        }
        Context ctx = runHandler(cmd, c, c.out, args);
        if (ctx != null && raft == null && cmd.has(CommandSpec.WRITE) && ctx.isDirty()) {
            persistence.append(ctx.propagation(args));
        }
    }

    /** Runs a command; a CommandException becomes an error reply. Returns null if it failed. */
    private Context runHandler(CommandSpec cmd, Connection c, RespWriter out, List<byte[]> args) {
        Context ctx = new Context(this, c, out);
        int mark = out.length();
        try {
            cmd.handler().run(ctx, args);
            return ctx;
        } catch (CommandException e) {
            out.truncate(mark);
            out.error(e.getMessage());
            return null;
        }
    }

    /** Cluster mode: a read on this node's memory. Must not change anything, not even lazy expiry. */
    void executeRead(CommandSpec cmd, Connection c, List<byte[]> args) {
        db.setReadOnly(true);
        try {
            runHandler(cmd, c, c.out, args);
        } finally {
            db.setReadOnly(false);
        }
    }

    /** Cluster mode: applies a committed command. {@code c} is the waiting client, or null. */
    void executeReplicated(List<byte[]> args, Connection c, RespWriter out) {
        CommandSpec cmd = commands.lookup(args.get(0));
        if (cmd == null || !cmd.arityMatches(args.size())) {
            out.error("ERR invalid command in the Raft log: " + Args.printable(args.get(0)));
            return;
        }
        runHandler(cmd, c, out, args);
    }

    /** Cluster mode: continue a client whose earlier command finished. */
    void resume(Connection c) {
        List<byte[]> deferred = c.deferred;
        c.deferred = null;
        if (deferred != null) execute(c, deferred);
        processInput(c);
    }

    /** Re-runs a command read from the append-only file. No client, no reply, no re-logging. */
    private void replay(List<byte[]> args) {
        CommandSpec cmd = commands.lookup(args.get(0));
        if (cmd == null || !cmd.arityMatches(args.size())) {
            throw new IllegalStateException("append-only file contains an invalid command: " + Args.printable(args.get(0)));
        }
        try {
            cmd.handler().run(new Context(this, null, replaySink), args);
        } catch (CommandException e) {
            Log.warn("replaying '%s' failed: %s", cmd.name(), e.getMessage());
        } finally {
            replaySink.reset();
        }
    }

    /**
     * Group commit: flush this loop pass's writes to the log (one fsync for all of them in
     * {@code always} mode), and only then send replies.
     */
    private void beforeSleep() throws IOException {
        if (raft == null) {
            persistence.flushBeforeReplies();
        } else {
            // 1. continue clients whose previous command finished (they may propose more writes)
            // 2. put this pass's Raft changes on disk with one fsync (group commit)
            // 3. only then tell Raft they're durable and send messages to the other nodes
            // In a one-node cluster, step 2 alone commits writes, which can unblock more clients,
            // so repeat a few times before sleeping.
            for (int round = 0; round < 32; round++) {
                raftKv.resumeReady();
                raftStorage.sync();
                raft.persisted();
                raftKv.checkLeadership();
                if (!raftKv.hasResumable()) break;
            }
            for (Message m : raft.drainOutbox()) transport.send(m);
            transport.flush(System.currentTimeMillis());
        }
        if (pendingWrites.isEmpty()) return;
        List<Connection> batch = new ArrayList<>(pendingWrites);
        pendingWrites.clear();
        for (Connection c : batch) {
            if (c.isOpen()) flush(c);
        }
    }

    void queueWrite(Connection c) {
        if (c.isOpen()) pendingWrites.add(c);
    }

    private void flush(Connection c) {
        try {
            if (c.flush()) {
                if (c.shouldCloseAfterReply()) close(c);
                else c.setWriteInterest(false);
            } else if (c.isSubscribed() && c.pendingOutput() > PUBSUB_OUTPUT_LIMIT) {
                Log.warn("closing client %d: it is reading pub/sub messages too slowly", c.id());
                close(c);
            } else {
                c.setWriteInterest(true); // socket is full; the selector will tell us when to retry
            }
        } catch (IOException e) {
            close(c);
        }
    }

    private void close(Connection c) {
        if (!c.isOpen()) return;
        pubsub.unsubscribeAll(c);
        clients.remove(c);
        pendingWrites.remove(c);
        c.close();
    }

    private void closeEverything() {
        for (Connection c : new ArrayList<>(clients)) close(c);
        closeQuietly();
        try {
            if (raft != null) {
                transport.close();
                raftStorage.close();
            } else {
                persistence.close(db);
            }
            Log.info("raftis is now ready to exit, bye bye...");
        } catch (IOException | RuntimeException e) {
            Log.warn("error while saving data on shutdown: %s", e);
        }
    }

    private void closeQuietly() {
        try {
            if (listener != null) listener.close();
            if (selector != null) selector.close();
        } catch (IOException ignored) {
            // shutting down anyway
        }
    }

    // ---- accessors used by commands (INFO, CONFIG GET...) ----

    public Keyspace db() {
        return db;
    }

    public CommandTable commands() {
        return commands;
    }

    public PubSub pubsub() {
        return pubsub;
    }

    public Persistence persistence() {
        return persistence;
    }

    public int port() {
        return port;
    }

    public int clientCount() {
        return clients.size();
    }

    public long totalCommands() {
        return totalCommands;
    }

    public long totalConnections() {
        return totalConnections;
    }

    public long uptimeSeconds() {
        return (System.currentTimeMillis() - startedAt) / 1000;
    }

    /** The Raft node in cluster mode, or null for a standalone server. */
    public RaftNode raft() {
        return raft;
    }

    public String leaderAddress() {
        return raftKv == null ? "" : raftKv.leaderAddress();
    }

    public int connectedPeers() {
        return transport == null ? 0 : transport.connectedPeers();
    }

    public Map<String, String> configParams() {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("bind", config.bind());
        params.put("port", Integer.toString(port));
        params.put("dir", config.dir().toAbsolutePath().toString());
        params.put("appendonly", config.appendOnly() ? "yes" : "no");
        params.put("appendfsync", config.fsync().configName());
        params.put("save", "");
        return params;
    }
}
