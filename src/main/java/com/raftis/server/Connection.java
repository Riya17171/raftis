package com.raftis.server;

import com.raftis.protocol.RespWriter;
import com.raftis.store.ByteKey;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * One client connection: its socket, the bytes received but not yet parsed, and the replies
 * waiting to be sent.
 */
public final class Connection {
    private static final int INITIAL_INPUT = 16 * 1024;
    private static final int MAX_INPUT = 1024 * 1024 * 1024; // like Redis's client-query-buffer-limit

    private final long id;
    private final SocketChannel channel;
    SelectionKey key;

    // Unparsed input lives in in[inStart, inEnd).
    byte[] in = new byte[INITIAL_INPUT];
    int inStart;
    int inEnd;

    /** Replies waiting to be sent. Commands append here; the event loop writes it to the socket. */
    public final RespWriter out = new RespWriter();
    private int outPos;

    private final Set<ByteKey> subscriptions = new HashSet<>();
    private boolean closeAfterReply;
    private boolean open = true;
    private String name;

    // Cluster mode. Replies must go out in the order commands arrived, so:
    //  - consecutive writes are proposed right away (Raft applies them in log order, so their
    //    replies come back in order), and inflightWrites counts the ones not yet answered;
    //  - any other command waits (deferred) until those writes are answered;
    //  - while blocked, no further input is parsed.
    private ReadMode readMode = ReadMode.CONSISTENT;
    int inflightWrites;
    List<byte[]> deferred;
    boolean blocked;

    Connection(long id, SocketChannel channel) {
        this.id = id;
        this.channel = channel;
    }

    public long id() {
        return id;
    }

    public String name() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ReadMode readMode() {
        return readMode;
    }

    public void setReadMode(ReadMode readMode) {
        this.readMode = readMode;
    }

    public boolean isSubscribed() {
        return !subscriptions.isEmpty();
    }

    public Set<ByteKey> subscriptions() {
        return Collections.unmodifiableSet(subscriptions);
    }

    Set<ByteKey> mutableSubscriptions() {
        return subscriptions;
    }

    /** Send pending replies, then close (QUIT, protocol errors). */
    public void closeAfterReply() {
        closeAfterReply = true;
    }

    boolean shouldCloseAfterReply() {
        return closeAfterReply;
    }

    boolean isOpen() {
        return open;
    }

    /** Reads whatever the socket has into the input buffer. Returns -1 when the client disconnected. */
    int readFrom() throws IOException {
        if (inEnd == in.length) {
            if (inStart > 0) {
                compact();
            } else {
                if (in.length >= MAX_INPUT) throw new IOException("client exceeded the query buffer limit");
                in = Arrays.copyOf(in, in.length * 2);
            }
        }
        int n = channel.read(ByteBuffer.wrap(in, inEnd, in.length - inEnd));
        if (n > 0) inEnd += n;
        return n;
    }

    /** Called after parsing: drops consumed bytes so the buffer doesn't grow forever. */
    void afterParse() {
        if (inStart == inEnd) {
            inStart = 0;
            inEnd = 0;
            if (in.length > 1024 * 1024) in = new byte[INITIAL_INPUT]; // give back memory after a huge command
        } else if (inStart > in.length / 2) {
            compact();
        }
    }

    private void compact() {
        System.arraycopy(in, inStart, in, 0, inEnd - inStart);
        inEnd -= inStart;
        inStart = 0;
    }

    /**
     * Writes as much pending output as the socket accepts.
     *
     * @return true if everything was sent, false if the socket's send buffer is full
     */
    boolean flush() throws IOException {
        while (outPos < out.length()) {
            int n = channel.write(ByteBuffer.wrap(out.buffer(), outPos, out.length() - outPos));
            if (n == 0) return false;
            outPos += n;
        }
        out.reset();
        outPos = 0;
        return true;
    }

    int pendingOutput() {
        return out.length() - outPos;
    }

    /** Ask the selector to tell us when the socket can take more data (only while output is stuck). */
    void setWriteInterest(boolean on) {
        if (key.isValid()) key.interestOps(on ? SelectionKey.OP_READ | SelectionKey.OP_WRITE : SelectionKey.OP_READ);
    }

    void close() {
        open = false;
        key.cancel();
        try {
            channel.close();
        } catch (IOException ignored) {
            // nothing useful to do
        }
    }
}
