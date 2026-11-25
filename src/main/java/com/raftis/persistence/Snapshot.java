package com.raftis.persistence;

import com.raftis.store.ByteKey;
import com.raftis.store.Keyspace;
import com.raftis.store.RHash;
import com.raftis.store.RList;
import com.raftis.store.ZSet;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Map;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;

/**
 * The snapshot file format:
 *
 * <pre>
 * "RAFTIS" | version:int | nextAofGeneration:long | createdAtMs:long
 * entries:   type:byte | expireAtMs:long (-1 = none) | key | value
 * 0xFF (end marker)
 * crc32:long   (covers everything before it)
 * </pre>
 *
 * Byte strings are written as length:int followed by the bytes.
 *
 * <p>The file is written to a temp file, fsynced, then atomically renamed over the old one, so
 * a crash mid-write can never destroy the previous good snapshot. On load the CRC is checked
 * first: a corrupted snapshot is refused rather than half-loaded.
 *
 * <p>Note: Redis takes snapshots in the background by calling fork() and letting the OS share
 * memory copy-on-write. The JVM can't fork, so SAVE here blocks the server while it writes.
 */
final class Snapshot {
    private static final byte[] MAGIC = {'R', 'A', 'F', 'T', 'I', 'S'};
    private static final int VERSION = 1;
    private static final int STRING = 0, LIST = 1, HASH = 2, ZSET = 3, END = 0xFF;

    private Snapshot() {}

    static void write(Path dir, String name, Keyspace db, long nextGeneration) throws IOException {
        Path tmp = dir.resolve("temp-" + ProcessHandle.current().pid() + ".snapshot");
        CRC32 crc = new CRC32();
        try (FileOutputStream file = new FileOutputStream(tmp.toFile())) {
            DataOutputStream out = new DataOutputStream(
                    new BufferedOutputStream(new CheckedOutputStream(file, crc), 1 << 16));
            out.write(MAGIC);
            out.writeInt(VERSION);
            out.writeLong(nextGeneration);
            out.writeLong(System.currentTimeMillis());
            for (Map.Entry<ByteKey, Object> e : db.entries()) {
                ByteKey key = e.getKey();
                if (db.isExpired(key)) continue;
                writeEntry(out, key, db.expireAt(key), e.getValue());
            }
            out.writeByte(END);
            out.flush();
            file.write(ByteBuffer.allocate(Long.BYTES).putLong(crc.getValue()).array());
            file.getFD().sync();
        } catch (IOException | RuntimeException e) {
            Files.deleteIfExists(tmp);
            throw e;
        }
        Files.move(tmp, dir.resolve(name), StandardCopyOption.ATOMIC_MOVE);
    }

    private static void writeEntry(DataOutputStream out, ByteKey key, long expireAt, Object value) throws IOException {
        switch (value) {
            case byte[] s -> {
                header(out, STRING, expireAt, key);
                bytes(out, s);
            }
            case RList list -> {
                header(out, LIST, expireAt, key);
                out.writeInt(list.size());
                for (int i = 0; i < list.size(); i++) bytes(out, list.get(i));
            }
            case RHash hash -> {
                header(out, HASH, expireAt, key);
                out.writeInt(hash.size());
                for (Map.Entry<ByteKey, byte[]> f : hash.entrySet()) {
                    bytes(out, f.getKey().bytes());
                    bytes(out, f.getValue());
                }
            }
            case ZSet zset -> {
                header(out, ZSET, expireAt, key);
                out.writeInt(zset.size());
                for (ZSet.Node n = zset.first(); n != null; n = n.next()) {
                    bytes(out, n.member());
                    out.writeDouble(n.score());
                }
            }
            default -> throw new IllegalStateException("unknown value type " + value.getClass());
        }
    }

    private static void header(DataOutputStream out, int type, long expireAt, ByteKey key) throws IOException {
        out.writeByte(type);
        out.writeLong(expireAt);
        bytes(out, key.bytes());
    }

    private static void bytes(DataOutputStream out, byte[] b) throws IOException {
        out.writeInt(b.length);
        out.write(b);
    }

    /** Loads a snapshot into {@code db}. Returns the AOF generation that follows it. */
    static long read(Path path, Keyspace db) throws IOException {
        byte[] all = Files.readAllBytes(path);
        int bodyLength = all.length - Long.BYTES;
        if (bodyLength < MAGIC.length + Integer.BYTES + 2 * Long.BYTES + 1) {
            throw new IOException("snapshot " + path + " is truncated");
        }
        CRC32 crc = new CRC32();
        crc.update(all, 0, bodyLength);
        long stored = ByteBuffer.wrap(all, bodyLength, Long.BYTES).getLong();
        if (crc.getValue() != stored) {
            throw new IOException("snapshot " + path + " failed its CRC32 check; refusing to load corrupted data");
        }
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(all, 0, bodyLength));
        if (!Arrays.equals(in.readNBytes(MAGIC.length), MAGIC)) throw new IOException(path + " is not a raftis snapshot");
        int version = in.readInt();
        if (version != VERSION) throw new IOException("unsupported snapshot version " + version);
        long nextGeneration = in.readLong();
        in.readLong(); // creation time, informational

        while (true) {
            int type = in.readUnsignedByte();
            if (type == END) break;
            long expireAt = in.readLong();
            ByteKey key = new ByteKey(bytes(in));
            Object value = switch (type) {
                case STRING -> bytes(in);
                case LIST -> {
                    RList list = new RList();
                    for (int i = in.readInt(); i > 0; i--) list.addLast(bytes(in));
                    yield list;
                }
                case HASH -> {
                    RHash hash = new RHash();
                    for (int i = in.readInt(); i > 0; i--) hash.put(new ByteKey(bytes(in)), bytes(in));
                    yield hash;
                }
                case ZSET -> {
                    ZSet zset = new ZSet();
                    for (int i = in.readInt(); i > 0; i--) zset.add(bytes(in), in.readDouble());
                    yield zset;
                }
                default -> throw new IOException("snapshot contains unknown type " + type);
            };
            db.put(key, value);
            if (expireAt >= 0) db.setExpireAt(key, expireAt);
        }
        return nextGeneration;
    }

    private static byte[] bytes(DataInputStream in) throws IOException {
        int n = in.readInt();
        if (n < 0 || n > in.available()) throw new IOException("snapshot has a corrupt length field");
        return in.readNBytes(n);
    }
}
