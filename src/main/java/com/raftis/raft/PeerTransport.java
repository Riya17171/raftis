package com.raftis.raft;

import com.raftis.Log;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.UnresolvedAddressException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Sends Raft messages between nodes over TCP, using the server's own NIO selector, so Raft
 * networking runs on the same single event-loop thread as everything else.
 *
 * <p>Each node opens one outgoing connection to every peer (only for sending) and accepts
 * incoming connections (only for receiving). Messages are framed as {@code [length:int][bytes]}.
 *
 * <p>Raft is designed to tolerate lost messages, so the transport stays simple: if a peer is
 * unreachable, messages to it are dropped and we keep trying to reconnect. The leader's next
 * heartbeat retransmits whatever is missing.
 */
public final class PeerTransport {
    /** Marks selection-key attachments that belong to the transport. */
    public sealed interface Endpoint permits Acceptor, Inbound, Outbound {}

    private static final int MAX_FRAME = 1024 * 1024 * 1024;
    private static final long RECONNECT_DELAY_MS = 100;
    private static final long MAX_QUEUED_BYTES = 64L * 1024 * 1024;

    private final Selector selector;
    private final Consumer<Message> deliver;
    private final Map<Integer, Outbound> outbound = new HashMap<>();
    private final Set<Inbound> inbound = new HashSet<>();
    private ServerSocketChannel listener;

    public PeerTransport(Selector selector, Map<Integer, InetSocketAddress> peers, Consumer<Message> deliver) {
        this.selector = selector;
        this.deliver = deliver;
        peers.forEach((id, address) -> outbound.put(id, new Outbound(id, address)));
    }

    public void bind(InetSocketAddress address) throws IOException {
        listener = ServerSocketChannel.open();
        listener.bind(address);
        listener.configureBlocking(false);
        listener.register(selector, SelectionKey.OP_ACCEPT, new Acceptor());
    }

    public void send(Message m) {
        Outbound peer = outbound.get(m.to());
        if (peer != null) peer.enqueue(MessageCodec.encode(m));
    }

    /** Writes queued messages and reconnects to peers that are down. Called once per loop pass. */
    public void flush(long now) {
        for (Outbound peer : outbound.values()) peer.flush(now);
    }

    public void onReady(SelectionKey key) {
        switch ((Endpoint) key.attachment()) {
            case Acceptor a -> accept();
            case Inbound in -> in.onReadable();
            case Outbound out -> out.onReady(key);
        }
    }

    /** Peers we currently have an outgoing connection to. */
    public int connectedPeers() {
        int n = 0;
        for (Outbound peer : outbound.values()) if (peer.connected) n++;
        return n;
    }

    public void close() {
        try {
            if (listener != null) listener.close();
        } catch (IOException ignored) {
            // shutting down
        }
        for (Inbound in : Set.copyOf(inbound)) in.close();
        for (Outbound out : outbound.values()) out.disconnect(0);
    }

    private void accept() {
        try {
            SocketChannel channel;
            while ((channel = listener.accept()) != null) {
                channel.configureBlocking(false);
                channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
                Inbound in = new Inbound(channel);
                in.key = channel.register(selector, SelectionKey.OP_READ, in);
                inbound.add(in);
            }
        } catch (IOException e) {
            Log.warn("raft: accepting a peer connection failed: %s", e);
        }
    }

    /** Placeholder attachment for the listening socket. */
    final class Acceptor implements Endpoint {}

    /** A connection a peer opened to us. We only read messages from it. */
    final class Inbound implements Endpoint {
        private final SocketChannel channel;
        private SelectionKey key;
        private byte[] buf = new byte[64 * 1024];
        private int start;
        private int end;

        Inbound(SocketChannel channel) {
            this.channel = channel;
        }

        void onReadable() {
            try {
                if (end == buf.length) makeRoom(end - start + 1);
                int n = channel.read(ByteBuffer.wrap(buf, end, buf.length - end));
                if (n < 0) {
                    close();
                    return;
                }
                end += n;
                while (end - start >= 4) {
                    int length = ByteBuffer.wrap(buf, start, 4).getInt();
                    if (length <= 0 || length > MAX_FRAME) throw new IOException("bad frame length " + length);
                    if (end - start - 4 < length) {
                        if (4 + length > buf.length - start) makeRoom(4 + length);
                        break;
                    }
                    Message m = MessageCodec.decode(buf, start + 4, length);
                    start += 4 + length;
                    deliver.accept(m);
                }
                if (start == end) start = end = 0;
            } catch (IOException | RuntimeException e) {
                close();
            }
        }

        /** Ensures {@code needed} bytes fit from {@code start}, compacting or growing the buffer. */
        private void makeRoom(int needed) {
            if (start > 0) {
                System.arraycopy(buf, start, buf, 0, end - start);
                end -= start;
                start = 0;
            }
            if (needed > buf.length) buf = Arrays.copyOf(buf, Math.max(needed, buf.length * 2));
        }

        void close() {
            inbound.remove(this);
            if (key != null) key.cancel();
            try {
                channel.close();
            } catch (IOException ignored) {
                // nothing to do
            }
        }
    }

    /** Our connection to a peer. We only write messages to it. */
    final class Outbound implements Endpoint {
        private final int peerId;
        private final String host;
        private final int port;
        private final ArrayDeque<ByteBuffer> queue = new ArrayDeque<>();
        private final ByteBuffer scratch = ByteBuffer.allocate(256);
        private SocketChannel channel;
        private SelectionKey key;
        private boolean connected;
        private long queuedBytes;
        private long retryAt;
        private boolean warnedDown;

        Outbound(int peerId, InetSocketAddress address) {
            this.peerId = peerId;
            this.host = address.getHostString();
            this.port = address.getPort();
        }

        void enqueue(byte[] message) {
            if (!connected) return;                                       // unreachable: drop, Raft retries
            if (queuedBytes + message.length + 4 > MAX_QUEUED_BYTES) return; // peer too slow: drop
            ByteBuffer frame = ByteBuffer.allocate(4 + message.length).putInt(message.length).put(message);
            frame.flip();
            queue.add(frame);
            queuedBytes += frame.remaining();
        }

        void flush(long now) {
            if (channel == null) {
                if (now >= retryAt) connect(now);
                return;
            }
            if (!connected) return;
            try {
                write();
            } catch (IOException e) {
                disconnect(now);
            }
        }

        private void connect(long now) {
            try {
                channel = SocketChannel.open();
                channel.configureBlocking(false);
                channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
                // Look the host name up again on every attempt: in Docker or Kubernetes a peer may not
                // be in DNS yet when we start, and a restarted peer can come back with a new IP address.
                if (channel.connect(new InetSocketAddress(host, port))) {
                    onConnected();
                } else {
                    key = channel.register(selector, SelectionKey.OP_CONNECT, this);
                }
            } catch (IOException | UnresolvedAddressException e) {
                disconnect(now);
            }
        }

        void onReady(SelectionKey k) {
            try {
                if (k.isConnectable()) {
                    if (channel.finishConnect()) onConnected();
                    return;
                }
                if (k.isReadable()) {
                    scratch.clear();
                    if (channel.read(scratch) < 0) throw new IOException("peer closed the connection");
                }
                if (k.isValid() && k.isWritable()) write();
            } catch (IOException e) {
                disconnect(System.currentTimeMillis());
            }
        }

        private void onConnected() throws IOException {
            connected = true;
            key = channel.register(selector, SelectionKey.OP_READ, this); // read only to notice when it closes
            if (warnedDown) Log.info("raft: connected to node %d at %s:%d", peerId, host, port);
            warnedDown = false;
        }

        private void write() throws IOException {
            while (!queue.isEmpty()) {
                ByteBuffer frame = queue.peek();
                channel.write(frame);
                if (frame.hasRemaining()) {
                    key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
                    return;
                }
                queuedBytes -= frame.limit();
                queue.poll();
            }
            key.interestOps(SelectionKey.OP_READ);
        }

        void disconnect(long now) {
            if (connected && !warnedDown) {
                Log.warn("raft: lost connection to node %d at %s:%d", peerId, host, port);
                warnedDown = true;
            }
            connected = false;
            if (key != null) key.cancel();
            if (channel != null) {
                try {
                    channel.close();
                } catch (IOException ignored) {
                    // nothing to do
                }
            }
            channel = null;
            key = null;
            queue.clear();
            queuedBytes = 0;
            retryAt = now + RECONNECT_DELAY_MS;
        }
    }
}
