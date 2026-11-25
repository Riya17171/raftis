package com.raftis.persistence;

import com.raftis.Log;
import com.raftis.protocol.ProtocolException;
import com.raftis.protocol.RespParser;
import com.raftis.protocol.RespWriter;
import com.raftis.store.Keyspace;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Durability: an append-only file (AOF) of every write, plus snapshots.
 *
 * <p><b>The AOF</b> holds write commands in RESP, exactly as a client would send them.
 * Recovery replays it through the normal command handlers.
 *
 * <p><b>Snapshots</b> ({@code SAVE}) write the whole dataset to {@code dump.snapshot} so the
 * AOF can start over instead of growing forever.
 *
 * <p><b>Generations.</b> Each AOF file is named {@code appendonly.<generation>.aof} and the
 * snapshot records which generation follows it. That makes SAVE crash-safe:
 * <ol>
 *   <li>create the empty next-generation AOF</li>
 *   <li>write the snapshot (tagged with the new generation) to a temp file, fsync it, and
 *       atomically rename it into place: <b>this rename is the commit point</b></li>
 *   <li>switch writes to the new AOF and delete the old one</li>
 * </ol>
 * A crash before step 2 finishes leaves the old snapshot pointing at the old AOF, which is
 * still complete. A crash after it leaves the new snapshot pointing at the new AOF. Either way,
 * no write is applied twice or lost. (Redis 7 solved the same problem with a "manifest" file.)
 */
public final class Persistence {
    public static final String SNAPSHOT_FILE = "dump.snapshot";
    private static final Pattern AOF_NAME = Pattern.compile("appendonly\\.(\\d+)\\.aof");

    private final Path dir;
    private final boolean aofEnabled;
    private final FsyncPolicy fsync;
    private final RespWriter pending = new RespWriter(64 * 1024);
    private final AtomicBoolean fsyncNeeded = new AtomicBoolean();
    private volatile FileChannel aof;
    private ScheduledExecutorService fsyncThread;
    private long generation = 1;
    private long lastSaveMillis;

    public Persistence(Path dir, boolean aofEnabled, FsyncPolicy fsync) {
        this.dir = dir;
        this.aofEnabled = aofEnabled;
        this.fsync = fsync;
    }

    /** Loads the snapshot (if any), then replays the AOF written after it. */
    public void load(Keyspace db, Consumer<List<byte[]>> replay) throws IOException {
        Files.createDirectories(dir);
        deleteMatching("temp-*.snapshot"); // a SAVE that crashed before its commit point
        db.setLoading(true);
        try {
            Path snapshot = dir.resolve(SNAPSHOT_FILE);
            if (Files.exists(snapshot)) {
                generation = Snapshot.read(snapshot, db);
                lastSaveMillis = Files.getLastModifiedTime(snapshot).toMillis();
                Log.info("loaded snapshot: %d keys (next AOF generation %d)", db.size(), generation);
            }
            Path log = aofPath(generation);
            if (Files.exists(log)) replayAof(log, replay);
        } finally {
            db.setLoading(false);
        }
        deleteStaleAofs();
        if (aofEnabled) openAof();
    }

    private void replayAof(Path path, Consumer<List<byte[]>> replay) throws IOException {
        byte[] data = Files.readAllBytes(path);
        int pos = 0;
        long commands = 0;
        while (pos < data.length) {
            if (data[pos] != '*') throw new IOException(path + " is corrupt at offset " + pos);
            RespParser.Result parsed;
            try {
                parsed = RespParser.parse(data, pos, data.length);
            } catch (ProtocolException e) {
                throw new IOException(path + " is corrupt at offset " + pos + ": " + e.getMessage());
            }
            if (parsed == null) {
                // The server crashed in the middle of writing a command. That command was never
                // acknowledged (we only reply after writing), so dropping it is safe.
                Log.warn("%s ends with an incomplete command (offset %d of %d); truncating it. "
                        + "This is expected after a crash.", path.getFileName(), pos, data.length);
                try (FileChannel ch = FileChannel.open(path, StandardOpenOption.WRITE)) {
                    ch.truncate(pos);
                    ch.force(true);
                }
                break;
            }
            if (!parsed.args().isEmpty()) {
                replay.accept(parsed.args());
                commands++;
            }
            pos = parsed.next();
        }
        Log.info("replayed %d commands from %s", commands, path.getFileName());
    }

    /** Queues a write command. It reaches the file in {@link #flushBeforeReplies()}. */
    public void append(List<byte[]> command) {
        if (aof != null) pending.command(command);
    }

    /**
     * Writes everything queued during this event-loop pass, and fsyncs in {@code always} mode.
     * The server calls this before sending any replies, so an acknowledged write is always in
     * the file. One fsync covers the whole batch: group commit.
     */
    public void flushBeforeReplies() throws IOException {
        FileChannel ch = aof;
        if (ch == null || pending.length() == 0) return;
        ByteBuffer buf = ByteBuffer.wrap(pending.buffer(), 0, pending.length());
        while (buf.hasRemaining()) ch.write(buf);
        pending.reset();
        switch (fsync) {
            case ALWAYS -> ch.force(false);
            case EVERYSEC -> fsyncNeeded.set(true);
            case NO -> { }
        }
    }

    /** SAVE: snapshot the dataset and start a fresh AOF generation. See the class comment. */
    public void save(Keyspace db) throws IOException {
        long t0 = System.nanoTime();
        flushBeforeReplies();
        long next = generation + 1;
        Path nextAof = aofPath(next);
        FileChannel nextChannel = null;
        try {
            if (aofEnabled) {
                Files.deleteIfExists(nextAof);
                nextChannel = FileChannel.open(nextAof,
                        StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
            }
            Snapshot.write(dir, SNAPSHOT_FILE, db, next); // commit point
        } catch (IOException | RuntimeException e) {
            if (nextChannel != null) nextChannel.close();
            Files.deleteIfExists(nextAof);
            throw e;
        }
        FileChannel old = aof;
        aof = nextChannel;
        generation = next;
        if (old != null) old.close();
        deleteStaleAofs();
        lastSaveMillis = System.currentTimeMillis();
        Log.info("DB saved on disk: %d keys in %.1f ms", db.size(), (System.nanoTime() - t0) / 1e6);
    }

    /** Clean shutdown: fsync the AOF, or take a snapshot if the AOF is disabled. */
    public void close(Keyspace db) throws IOException {
        if (fsyncThread != null) fsyncThread.shutdownNow();
        FileChannel ch = aof;
        if (ch != null) {
            flushBeforeReplies();
            ch.force(true);
            ch.close();
            aof = null;
        } else {
            save(db);
        }
    }

    private void openAof() throws IOException {
        aof = FileChannel.open(aofPath(generation),
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        if (fsync == FsyncPolicy.EVERYSEC) {
            fsyncThread = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "raftis-aof-fsync");
                t.setDaemon(true);
                return t;
            });
            // fsync can take milliseconds, so in everysec mode it runs off the event loop, like Redis.
            fsyncThread.scheduleWithFixedDelay(this::backgroundFsync, 1, 1, TimeUnit.SECONDS);
        }
    }

    private void backgroundFsync() {
        if (!fsyncNeeded.getAndSet(false)) return;
        FileChannel ch = aof;
        if (ch == null) return;
        try {
            ch.force(false);
        } catch (ClosedChannelException e) {
            // SAVE switched to a new file; it'll be fsynced next time
        } catch (IOException e) {
            Log.warn("background AOF fsync failed: %s", e);
        }
    }

    private Path aofPath(long gen) {
        return dir.resolve("appendonly." + gen + ".aof");
    }

    private void deleteStaleAofs() throws IOException {
        try (DirectoryStream<Path> files = Files.newDirectoryStream(dir, "appendonly.*.aof")) {
            for (Path file : files) {
                Matcher m = AOF_NAME.matcher(file.getFileName().toString());
                if (m.matches() && Long.parseLong(m.group(1)) != generation) {
                    Files.deleteIfExists(file);
                    Log.info("removed old %s", file.getFileName());
                }
            }
        }
    }

    private void deleteMatching(String glob) throws IOException {
        try (DirectoryStream<Path> files = Files.newDirectoryStream(dir, glob)) {
            for (Path file : files) Files.deleteIfExists(file);
        }
    }

    public boolean aofEnabled() {
        return aofEnabled;
    }

    public FsyncPolicy fsyncPolicy() {
        return fsync;
    }

    public long generation() {
        return generation;
    }

    public long lastSaveMillis() {
        return lastSaveMillis;
    }
}
