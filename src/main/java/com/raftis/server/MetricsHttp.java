package com.raftis.server;

import com.raftis.Log;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;

/**
 * A minimal HTTP server for {@code GET /metrics}, on the server's own selector and thread.
 *
 * <p>Because it runs on the event loop, it reads the live Raft and keyspace state directly: no
 * locks, no copies, and the numbers in one response are always consistent with each other. Each
 * request is answered in the loop pass where its headers arrive, then the connection is closed.
 */
final class MetricsHttp {
    /** Marks selection-key attachments that belong to this server. */
    sealed interface Endpoint permits Listener, Exchange {}

    private static final int MAX_REQUEST = 8 * 1024;

    private final Selector selector;
    private final Supplier<String> metrics;
    private final Set<Exchange> open = new HashSet<>();
    private ServerSocketChannel listener;

    MetricsHttp(Selector selector, Supplier<String> metrics) {
        this.selector = selector;
        this.metrics = metrics;
    }

    int bind(InetSocketAddress address) throws IOException {
        listener = ServerSocketChannel.open();
        listener.bind(address);
        listener.configureBlocking(false);
        listener.register(selector, SelectionKey.OP_ACCEPT, new Listener());
        return ((InetSocketAddress) listener.getLocalAddress()).getPort();
    }

    void onReady(SelectionKey key) {
        switch ((Endpoint) key.attachment()) {
            case Listener l -> accept();
            case Exchange x -> x.onReady(key);
        }
    }

    void close() {
        try {
            if (listener != null) listener.close();
        } catch (IOException ignored) {
            // shutting down
        }
        for (Exchange x : Set.copyOf(open)) x.close();
    }

    private void accept() {
        try {
            SocketChannel channel;
            while ((channel = listener.accept()) != null) {
                channel.configureBlocking(false);
                Exchange x = new Exchange(channel);
                x.key = channel.register(selector, SelectionKey.OP_READ, x);
                open.add(x);
            }
        } catch (IOException e) {
            Log.warn("metrics: accepting a connection failed: %s", e);
        }
    }

    final class Listener implements Endpoint {}

    /** One request and its response. */
    final class Exchange implements Endpoint {
        private final SocketChannel channel;
        private final ByteBuffer in = ByteBuffer.allocate(MAX_REQUEST);
        private ByteBuffer out;
        private SelectionKey key;

        Exchange(SocketChannel channel) {
            this.channel = channel;
        }

        void onReady(SelectionKey k) {
            try {
                if (out == null && k.isReadable()) {
                    if (channel.read(in) < 0) {
                        close();
                        return;
                    }
                    String request = new String(in.array(), 0, in.position(), StandardCharsets.ISO_8859_1);
                    if (request.contains("\r\n\r\n")) respond(request);
                    else if (!in.hasRemaining()) respond(null); // headers too large
                    else return;
                }
                if (out != null) {
                    channel.write(out);
                    if (out.hasRemaining()) key.interestOps(SelectionKey.OP_WRITE);
                    else close();
                }
            } catch (IOException e) {
                close();
            }
        }

        private void respond(String request) {
            String line = request == null ? "" : request.substring(0, request.indexOf("\r\n"));
            String[] parts = line.split(" ");
            String method = parts.length > 0 ? parts[0] : "";
            String path = parts.length > 1 ? parts[1].split("\\?")[0] : "";
            if (request == null) {
                out = response("431 Request Header Fields Too Large", "text/plain", "request too large\n");
            } else if (!method.equals("GET")) {
                out = response("405 Method Not Allowed", "text/plain", "only GET is supported\n");
            } else if (path.equals("/metrics")) {
                out = response("200 OK", "text/plain; version=0.0.4; charset=utf-8", metrics.get());
            } else if (path.equals("/")) {
                out = response("200 OK", "text/plain", "raftis: metrics are at /metrics\n");
            } else {
                out = response("404 Not Found", "text/plain", "not found\n");
            }
        }

        private ByteBuffer response(String status, String type, String body) {
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            String head = "HTTP/1.1 " + status + "\r\nContent-Type: " + type + "\r\nContent-Length: " + bytes.length
                    + "\r\nConnection: close\r\n\r\n";
            byte[] h = head.getBytes(StandardCharsets.ISO_8859_1);
            return ByteBuffer.allocate(h.length + bytes.length).put(h).put(bytes).flip();
        }

        void close() {
            open.remove(this);
            if (key != null) key.cancel();
            try {
                channel.close();
            } catch (IOException ignored) {
                // nothing to do
            }
        }
    }
}
