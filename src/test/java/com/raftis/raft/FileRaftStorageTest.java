package com.raftis.raft;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileRaftStorageTest {
    @TempDir
    Path dir;

    private static Entry entry(long index, long term, String data) {
        return new Entry(index, term, data == null ? null : data.getBytes(StandardCharsets.UTF_8));
    }

    private static List<String> describe(List<Entry> entries) {
        return entries.stream().map(e -> e.index() + "/" + e.term() + "/" + (e.data() == null ? "noop" : new String(e.data(), StandardCharsets.UTF_8))).toList();
    }

    @Test
    void stateLogAndTruncationSurviveAReopen() throws IOException {
        FileRaftStorage s = FileRaftStorage.open(dir);
        s.setHardState(new RaftStorage.HardState(7, 2));
        s.append(entry(1, 1, null));
        s.append(entry(2, 1, "a"));
        s.append(entry(3, 2, "b"));
        s.sync();
        s.append(entry(4, 2, "c")); // written but then truncated before and after a sync
        s.truncateFrom(3);
        s.append(entry(3, 3, "B"));
        s.close();

        FileRaftStorage reopened = FileRaftStorage.open(dir);
        assertEquals(new RaftStorage.HardState(7, 2), reopened.hardState());
        assertEquals(List.of("1/1/noop", "2/1/a", "3/3/B"), describe(reopened.entries()));
        assertNull(reopened.snapshot());
        reopened.close();
    }

    @Test
    void aTornLastRecordIsDiscarded() throws IOException {
        FileRaftStorage s = FileRaftStorage.open(dir);
        s.append(entry(1, 1, "a"));
        s.append(entry(2, 1, "b"));
        s.close();
        // Simulate a crash in the middle of writing a third record.
        Files.write(dir.resolve("raft-log"), new byte[] {0, 0, 0, 40, 1, 2, 3}, StandardOpenOption.APPEND);

        FileRaftStorage reopened = FileRaftStorage.open(dir);
        assertEquals(List.of("1/1/a", "2/1/b"), describe(reopened.entries()));
        reopened.append(entry(3, 1, "c")); // the log keeps working after the repair
        reopened.close();
        assertEquals(List.of("1/1/a", "2/1/b", "3/1/c"), describe(FileRaftStorage.open(dir).entries()));
    }

    @Test
    void snapshotsReplaceThePrefixOfTheLog() throws IOException {
        FileRaftStorage s = FileRaftStorage.open(dir);
        for (int i = 1; i <= 5; i++) s.append(entry(i, 1, "e" + i));
        s.sync();
        s.saveSnapshot(new RaftSnapshot(3, 1, "state@3".getBytes(StandardCharsets.UTF_8)),
                List.of(entry(4, 1, "e4"), entry(5, 1, "e5")));
        s.append(entry(6, 2, "e6"));
        s.close();

        FileRaftStorage reopened = FileRaftStorage.open(dir);
        assertEquals(3, reopened.snapshot().index());
        assertArrayEquals("state@3".getBytes(StandardCharsets.UTF_8), reopened.snapshot().data());
        assertEquals(List.of("4/1/e4", "5/1/e5", "6/2/e6"), describe(reopened.entries()));
    }

    @Test
    void corruptedStateIsRefused() throws IOException {
        FileRaftStorage s = FileRaftStorage.open(dir);
        s.setHardState(new RaftStorage.HardState(3, 1));
        s.close();
        byte[] bytes = Files.readAllBytes(dir.resolve("raft-state"));
        bytes[0] ^= 1;
        Files.write(dir.resolve("raft-state"), bytes);
        IOException e = assertThrows(IOException.class, () -> FileRaftStorage.open(dir));
        assertTrue(e.getMessage().contains("CRC32"));
    }

    @Test
    void messagesSurviveEncoding() {
        List<Message> messages = List.of(
                new Message.RequestVote(1, 2, 5, 10, 4),
                new Message.VoteReply(2, 1, 5, true),
                new Message.AppendEntries(1, 3, 5, 9, 4, List.of(entry(10, 5, null), entry(11, 5, "set")), 8, 42),
                new Message.AppendReply(3, 1, 5, false, 0, 7, 3, 42),
                new Message.SnapshotReply(3, 1, 5, 100, 43));
        for (Message m : messages) {
            byte[] bytes = MessageCodec.encode(m);
            assertEquals(m.toString().replaceAll("\\[B@\\w+", "bytes"),
                    MessageCodec.decode(bytes, 0, bytes.length).toString().replaceAll("\\[B@\\w+", "bytes"));
        }
        Message.InstallSnapshot snap = new Message.InstallSnapshot(1, 2, 6, 100, 5, new byte[] {1, 2, 3}, 9);
        byte[] bytes = MessageCodec.encode(snap);
        Message.InstallSnapshot decoded = (Message.InstallSnapshot) MessageCodec.decode(bytes, 0, bytes.length);
        assertEquals(100, decoded.lastIncludedIndex());
        assertArrayEquals(new byte[] {1, 2, 3}, decoded.data());
    }
}
