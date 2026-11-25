package com.raftis.raft;

import com.raftis.raft.Message.AppendEntries;
import com.raftis.raft.Message.AppendReply;
import com.raftis.raft.Message.InstallSnapshot;
import com.raftis.raft.Message.RequestVote;
import com.raftis.raft.Message.SnapshotReply;
import com.raftis.raft.Message.VoteReply;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

/** Binary encoding of Raft messages for the network. Every message starts with a type byte. */
public final class MessageCodec {
    private static final byte REQUEST_VOTE = 1, VOTE_REPLY = 2, APPEND = 3, APPEND_REPLY = 4,
            SNAPSHOT = 5, SNAPSHOT_REPLY = 6;

    private MessageCodec() {}

    public static byte[] encode(Message m) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(64);
        DataOutputStream out = new DataOutputStream(bytes);
        try {
            switch (m) {
                case RequestVote r -> {
                    header(out, REQUEST_VOTE, m);
                    out.writeLong(r.lastLogIndex());
                    out.writeLong(r.lastLogTerm());
                }
                case VoteReply r -> {
                    header(out, VOTE_REPLY, m);
                    out.writeBoolean(r.granted());
                }
                case AppendEntries a -> {
                    header(out, APPEND, m);
                    out.writeLong(a.prevLogIndex());
                    out.writeLong(a.prevLogTerm());
                    out.writeLong(a.leaderCommit());
                    out.writeLong(a.readSeq());
                    out.writeInt(a.entries().size());
                    for (Entry e : a.entries()) {
                        out.writeLong(e.index());
                        out.writeLong(e.term());
                        out.writeInt(e.data() == null ? -1 : e.data().length);
                        if (e.data() != null) out.write(e.data());
                    }
                }
                case AppendReply r -> {
                    header(out, APPEND_REPLY, m);
                    out.writeBoolean(r.success());
                    out.writeLong(r.matchIndex());
                    out.writeLong(r.conflictIndex());
                    out.writeLong(r.conflictTerm());
                    out.writeLong(r.readSeq());
                }
                case InstallSnapshot s -> {
                    header(out, SNAPSHOT, m);
                    out.writeLong(s.lastIncludedIndex());
                    out.writeLong(s.lastIncludedTerm());
                    out.writeLong(s.readSeq());
                    out.writeInt(s.data().length);
                    out.write(s.data());
                }
                case SnapshotReply r -> {
                    header(out, SNAPSHOT_REPLY, m);
                    out.writeLong(r.lastIncludedIndex());
                    out.writeLong(r.readSeq());
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return bytes.toByteArray();
    }

    public static Message decode(byte[] bytes, int offset, int length) {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes, offset, length));
        try {
            byte type = in.readByte();
            int from = in.readInt();
            int to = in.readInt();
            long term = in.readLong();
            return switch (type) {
                case REQUEST_VOTE -> new RequestVote(from, to, term, in.readLong(), in.readLong());
                case VOTE_REPLY -> new VoteReply(from, to, term, in.readBoolean());
                case APPEND -> {
                    long prevIndex = in.readLong();
                    long prevTerm = in.readLong();
                    long commit = in.readLong();
                    long readSeq = in.readLong();
                    int count = in.readInt();
                    List<Entry> entries = new ArrayList<>(count);
                    for (int i = 0; i < count; i++) {
                        long index = in.readLong();
                        long entryTerm = in.readLong();
                        int dataLength = in.readInt();
                        byte[] data = dataLength < 0 ? null : in.readNBytes(dataLength);
                        entries.add(new Entry(index, entryTerm, data));
                    }
                    yield new AppendEntries(from, to, term, prevIndex, prevTerm, entries, commit, readSeq);
                }
                case APPEND_REPLY -> new AppendReply(from, to, term, in.readBoolean(), in.readLong(),
                        in.readLong(), in.readLong(), in.readLong());
                case SNAPSHOT -> {
                    long index = in.readLong();
                    long snapshotTerm = in.readLong();
                    long readSeq = in.readLong();
                    byte[] data = in.readNBytes(in.readInt());
                    yield new InstallSnapshot(from, to, term, index, snapshotTerm, data, readSeq);
                }
                case SNAPSHOT_REPLY -> new SnapshotReply(from, to, term, in.readLong(), in.readLong());
                default -> throw new IllegalArgumentException("unknown message type " + type);
            };
        } catch (IOException e) {
            throw new IllegalArgumentException("malformed message", e);
        }
    }

    private static void header(DataOutputStream out, byte type, Message m) throws IOException {
        out.writeByte(type);
        out.writeInt(m.from());
        out.writeInt(m.to());
        out.writeLong(m.term());
    }
}
