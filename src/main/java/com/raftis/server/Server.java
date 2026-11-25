package com.raftis.server;

import com.raftis.Log;
import com.raftis.command.Args;
import com.raftis.command.CommandException;
import com.raftis.command.CommandSpec;
import com.raftis.command.CommandTable;
import com.raftis.command.Context;
import com.raftis.persistence.Persistence;
import com.raftis.protocol.ProtocolException;
import com.raftis.protocol.RespParser;
import com.raftis.protocol.RespWriter;
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
    public static final String VERSION = "0.1.0";
    private static final long EXPIRE_CYCLE_MS = 100;
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

    public Server(Config config) {
        this.config = config;
        this.persistence = new Persistence(config.dir(), config.appendOnly(), config.fsync());
    }

    /** Binds the port and loads data from disk. Call {@link #run()} afterwards. */
    public void start() throws IOException {
        try {
            selector = Selector.open();
            listener = ServerSocketChannel.open();
            listener.bind(new InetSocketAddress(config.bind(), config.port()), 511);
            listener.configureBlocking(false);
            listener.register(selector, SelectionKey.OP_ACCEPT);
            port = ((InetSocketAddress) listener.getLocalAddress()).getPort();

            long t0 = System.nanoTime();
            persistence.load(db, this::replay);
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
            long wait = Math.max(1, nextExpireCycle - System.currentTimeMillis());
            selector.select(wait);
            Set<SelectionKey> ready = selector.selectedKeys();
            for (SelectionKey key : ready) {
                if (!key.isValid()) continue;
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
            if (now >= nextExpireCycle) {
                db.activeExpireCycle();
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
    private void processInput(Connection c) {
        while (!c.shouldCloseAfterReply()) {
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
        totalCommands++;
        CommandSpec cmd = commands.lookup(args.get(0));
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
        Context ctx = new Context(this, c, c.out);
        int mark = c.out.length();
        try {
            cmd.handler().run(ctx, args);
        } catch (CommandException e) {
            c.out.truncate(mark);
            c.out.error(e.getMessage());
            return;
        }
        if (cmd.has(CommandSpec.WRITE) && ctx.isDirty()) persistence.append(ctx.propagation(args));
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
        persistence.flushBeforeReplies();
        if (pendingWrites.isEmpty()) return;
        List<Connection> batch = new ArrayList<>(pendingWrites);
        pendingWrites.clear();
        for (Connection c : batch) {
            if (c.isOpen()) flush(c);
        }
    }

    private void queueWrite(Connection c) {
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
            persistence.close(db);
            Log.info("raftis is now ready to exit, bye bye...");
        } catch (IOException e) {
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
