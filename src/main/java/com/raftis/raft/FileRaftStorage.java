package com.raftis.raft;

import com.raftis.Log;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.CRC32;

/**
 * Raft state on disk, in three files:
 * <ul>
 *   <li>{@code raft-state}: current term and vote. Rewritten atomically (temp file, fsync, rename).</li>
 *   <li>{@code raft-log}: append-only records {@code [length:int][crc32:int][index][term][type][data]}.
 *       A crash mid-append leaves a torn last record; on startup it fails its CRC and is cut off.
 *       It was never acknowledged, because nothing is sent before {@link #sync()}.</li>
 *   <li>{@code raft-snapshot}: {@code [index][term][length][data][crc32]}, written atomically.</li>
 * </ul>
 * Appends are buffered and written with one write and one fsync per {@link #sync()}, which
 * gives group commit: every entry from one event-loop pass shares a single fsync.
 *
 * <p>With {@code fsync = false} the log is written but never forced to disk. That survives a
 * process crash (the OS still has the data) but not a power cut, so it breaks Raft's guarantees.
 * It exists only to measure how much of the cost is the disk.
 */
public final class FileRaftStorage implements RaftStorage {
    private static final String STATE = "raft-state";
    private static final String LOG = "raft-log";
    private static final String SNAPSHOT = "raft-snapshot";
    private static final int RECORD_HEADER = 8;   // length + crc
    private static final int ENTRY_HEADER = 17;   // index + term + type

    private final Path dir;
    private final boolean fsync;
    private HardState hardState = new HardState(0, -1);
    private boolean hardStateDirty;
    private RaftSnapshot snapshot;
    private List<Entry> loaded;

    private FileChannel log;
    private long fileSize;                                    // bytes already in the file
    private final ArrayList<Long> offsets = new ArrayList<>(); // file offset of each entry, in order
    private long firstIndex;                                  // index of the entry at offsets[0]
    private byte[] buffer = new byte[64 * 1024];              // appended but not yet written
    private int bufferLength;
    private boolean logNeedsFsync;

    private FileRaftStorage(Path dir, boolean fsync) {
        this.dir = dir;
        this.fsync = fsync;
    }

    public static FileRaftStorage open(Path dir) throws IOException {
        return open(dir, true);
    }

    public static FileRaftStorage open(Path dir, boolean fsync) throws IOException {
        FileRaftStorage s = new FileRaftStorage(dir, fsync);
        s.load();
        return s;
    }

    private void load() throws IOException {
        Files.createDirectories(dir);
        try (DirectoryStream<Path> temps = Files.newDirectoryStream(dir, "*.tmp")) {
            for (Path p : temps) Files.deleteIfExists(p);
        }
        Path statePath = dir.resolve(STATE);
        if (Files.exists(statePath)) {
            ByteBuffer b = ByteBuffer.wrap(checked(Files.readAllBytes(statePath), statePath));
            hardState = new HardState(b.getLong(), b.getInt());
        }
        Path snapshotPath = dir.resolve(SNAPSHOT);
        if (Files.exists(snapshotPath)) {
            ByteBuffer b = ByteBuffer.wrap(checked(Files.readAllBytes(snapshotPath), snapshotPath));
            long index = b.getLong();
            long term = b.getLong();
            byte[] data = new byte[b.getInt()];
            b.get(data);
            snapshot = new RaftSnapshot(index, term, data);
        }
        loadLog();
    }

    private void loadLog() throws IOException {
        Path path = dir.resolve(LOG);
        byte[] all = Files.exists(path) ? Files.readAllBytes(path) : new byte[0];
        List<Entry> entries = new ArrayList<>();
        int pos = 0;
        while (all.length - pos >= RECORD_HEADER) {
            ByteBuffer header = ByteBuffer.wrap(all, pos, RECORD_HEADER);
            int length = header.getInt();
            int crc = header.getInt();
            if (length < ENTRY_HEADER || (long) pos + RECORD_HEADER + length > all.length) break;
            CRC32 c = new CRC32();
            c.update(all, pos + RECORD_HEADER, length);
            if ((int) c.getValue() != crc) break;
            entries.add(decodeEntry(all, pos + RECORD_HEADER, length));
            pos += RECORD_HEADER + length;
        }
        boolean rewrite = false;
        if (pos < all.length) {
            Log.warn("raft-log has a torn record at offset %d of %d (crash while writing); discarding it", pos, all.length);
            rewrite = true;
        }
        // A crash between saving a snapshot and rewriting the log leaves entries the snapshot already covers.
        long snapshotIndex = snapshot == null ? 0 : snapshot.index();
        if (entries.removeIf(e -> e.index() <= snapshotIndex)) rewrite = true;
        for (int i = 0; i < entries.size(); i++) {
            if (entries.get(i).index() != snapshotIndex + 1 + i) {
                throw new IOException("raft-log is not contiguous at index " + entries.get(i).index());
            }
        }
        if (rewrite || !Files.exists(path)) {
            rewriteLog(entries);
        } else {
            openLog(entries);
        }
        loaded = entries;
    }

    @Override
    public HardState hardState() {
        return hardState;
    }

    @Override
    public void setHardState(HardState state) {
        hardState = state;
        hardStateDirty = true;
    }

    @Override
    public RaftSnapshot snapshot() {
        return snapshot;
    }

    @Override
    public List<Entry> entries() {
        List<Entry> result = loaded == null ? List.of() : loaded;
        loaded = null;
        return result;
    }

    @Override
    public void append(Entry entry) {
        byte[] record = encodeRecord(entry);
        if (offsets.isEmpty()) firstIndex = entry.index();
        offsets.add(fileSize + bufferLength);
        if (bufferLength + record.length > buffer.length) {
            buffer = Arrays.copyOf(buffer, Math.max(buffer.length * 2, bufferLength + record.length));
        }
        System.arraycopy(record, 0, buffer, bufferLength, record.length);
        bufferLength += record.length;
    }

    @Override
    public void truncateFrom(long index) {
        if (offsets.isEmpty() || index > firstIndex + offsets.size() - 1) return;
        int position = (int) Math.max(0, index - firstIndex);
        long offset = offsets.get(position);
        if (offset >= fileSize) {
            bufferLength = (int) (offset - fileSize); // only unwritten bytes are affected
        } else {
            bufferLength = 0;
            try {
                log.truncate(offset);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            fileSize = offset;
            logNeedsFsync = true;
        }
        offsets.subList(position, offsets.size()).clear();
    }

    @Override
    public void saveSnapshot(RaftSnapshot s, List<Entry> remaining) {
        try {
            ByteBuffer b = ByteBuffer.allocate(20 + s.data().length);
            b.putLong(s.index()).putLong(s.term()).putInt(s.data().length).put(s.data());
            writeAtomically(SNAPSHOT, withCrc(b.array()));
            snapshot = s;
            rewriteLog(remaining); // after the snapshot is safe, so a crash in between loses nothing
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void sync() {
        try {
            if (bufferLength > 0) {
                ByteBuffer b = ByteBuffer.wrap(buffer, 0, bufferLength);
                long position = fileSize;
                while (b.hasRemaining()) position += log.write(b, position);
                fileSize += bufferLength;
                bufferLength = 0;
                logNeedsFsync = true;
            }
            if (logNeedsFsync) {
                if (fsync) log.force(false);
                logNeedsFsync = false;
            }
            if (hardStateDirty) {
                ByteBuffer b = ByteBuffer.allocate(12).putLong(hardState.term()).putInt(hardState.votedFor());
                writeAtomically(STATE, withCrc(b.array()));
                hardStateDirty = false;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() {
        try {
            sync();
            log.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // ------------------------------------------------------------ helpers

    private void openLog(List<Entry> entries) throws IOException {
        if (log != null) log.close();
        log = FileChannel.open(dir.resolve(LOG), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        offsets.clear();
        long offset = 0;
        for (Entry e : entries) {
            offsets.add(offset);
            offset += RECORD_HEADER + ENTRY_HEADER + (e.data() == null ? 0 : e.data().length);
        }
        firstIndex = entries.isEmpty() ? 0 : entries.get(0).index();
        fileSize = log.size();
        bufferLength = 0;
    }

    private void rewriteLog(List<Entry> entries) throws IOException {
        Path tmp = dir.resolve(LOG + ".tmp");
        try (FileChannel out = FileChannel.open(tmp, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            for (Entry e : entries) {
                ByteBuffer b = ByteBuffer.wrap(encodeRecord(e));
                while (b.hasRemaining()) out.write(b);
            }
            out.force(true);
        }
        if (log != null) {
            log.close();
            log = null;
        }
        Files.move(tmp, dir.resolve(LOG), StandardCopyOption.ATOMIC_MOVE);
        openLog(entries);
    }

    private void writeAtomically(String name, byte[] bytes) throws IOException {
        Path tmp = dir.resolve(name + ".tmp");
        try (FileChannel out = FileChannel.open(tmp, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            ByteBuffer b = ByteBuffer.wrap(bytes);
            while (b.hasRemaining()) out.write(b);
            out.force(true);
        }
        Files.move(tmp, dir.resolve(name), StandardCopyOption.ATOMIC_MOVE);
    }

    private static byte[] encodeRecord(Entry e) {
        int dataLength = e.data() == null ? 0 : e.data().length;
        ByteBuffer payload = ByteBuffer.allocate(ENTRY_HEADER + dataLength);
        payload.putLong(e.index()).putLong(e.term()).put((byte) (e.data() == null ? 0 : 1));
        if (e.data() != null) payload.put(e.data());
        CRC32 crc = new CRC32();
        crc.update(payload.array());
        return ByteBuffer.allocate(RECORD_HEADER + payload.capacity())
                .putInt(payload.capacity()).putInt((int) crc.getValue()).put(payload.array()).array();
    }

    private static Entry decodeEntry(byte[] bytes, int offset, int length) {
        ByteBuffer b = ByteBuffer.wrap(bytes, offset, length);
        long index = b.getLong();
        long term = b.getLong();
        byte type = b.get();
        byte[] data = null;
        if (type == 1) {
            data = new byte[length - ENTRY_HEADER];
            b.get(data);
        }
        return new Entry(index, term, data);
    }

    private static byte[] withCrc(byte[] body) {
        CRC32 crc = new CRC32();
        crc.update(body);
        return ByteBuffer.allocate(body.length + 8).put(body).putLong(crc.getValue()).array();
    }

    /** Returns the body of a file written by {@link #withCrc}, or throws if it's corrupted. */
    private static byte[] checked(byte[] file, Path path) throws IOException {
        if (file.length < 8) throw new IOException(path + " is truncated");
        CRC32 crc = new CRC32();
        crc.update(file, 0, file.length - 8);
        if (crc.getValue() != ByteBuffer.wrap(file, file.length - 8, 8).getLong()) {
            throw new IOException(path + " failed its CRC32 check; refusing to start with corrupted Raft state");
        }
        return Arrays.copyOf(file, file.length - 8);
    }
}
