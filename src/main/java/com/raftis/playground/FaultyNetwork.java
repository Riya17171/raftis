package com.raftis.playground;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Sits between the nodes: every directed Raft link (node A sending to node B) goes through its
 * own TCP proxy, so a test can cut any link on its own. Nodes are just told that B lives at the
 * proxy's port.
 *
 * <p>A cut link behaves like a real network partition, not like a crashed peer: the sender's
 * connection stays open and its bytes simply vanish, so nothing tells it the peer is gone; it only
 * notices through timeouts, exactly like with a firewall dropping packets. When the link is healed
 * the silent connections are closed (as TCP eventually would) and the sender reconnects.
 *
 * <p>Each link carries traffic in one direction only (raftis opens one connection per peer, for
 * sending), so cutting A&rarr;B but not B&rarr;A gives a one-way partition.
 *
 * <p>Used by the chaos test and by the playground.
 */
public final class FaultyNetwork implements AutoCloseable {
    private final Map<Long, Link> links = new ConcurrentHashMap<>();

    /** Creates the proxy for messages from {@code from} to {@code to}; returns the port {@code from} should dial. */
    public int addLink(int from, int to, int targetPort) throws IOException {
        Link link = new Link(from, to, targetPort);
        links.put(key(from, to), link);
        return link.port();
    }

    public boolean isCut(int from, int to) {
        return links.get(key(from, to)).cut;
    }

    public void cut(int from, int to) {
        links.get(key(from, to)).cut();
    }

    public void heal(int from, int to) {
        links.get(key(from, to)).heal();
    }

    /** Bytes delivered from {@code from} to {@code to} so far (not counting dropped ones). */
    public long bytesDelivered(int from, int to) {
        return links.get(key(from, to)).delivered.get();
    }

    /** Cuts every link between nodes that share no group. A node may be in several groups (a "bridge"). */
    public void partition(Collection<Set<Integer>> groups) {
        for (Link link : links.values()) {
            boolean together = groups.stream().anyMatch(g -> g.contains(link.from) && g.contains(link.to));
            if (!together) link.cut();
        }
    }

    public void healAll() {
        links.values().forEach(Link::heal);
    }

    @Override
    public void close() {
        links.values().forEach(Link::close);
    }

    private static long key(int from, int to) {
        return ((long) from << 32) | to;
    }

    private static final class Link {
        final int from;
        final int to;
        private final int targetPort;
        private final ServerSocket server;
        private final Set<Pipe> pipes = ConcurrentHashMap.newKeySet();
        private final AtomicLong delivered = new AtomicLong();
        volatile boolean cut;

        Link(int from, int to, int targetPort) throws IOException {
            this.from = from;
            this.to = to;
            this.targetPort = targetPort;
            this.server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
            Thread.ofVirtual().name("link-" + from + "-" + to).start(this::acceptLoop);
        }

        int port() {
            return server.getLocalPort();
        }

        synchronized void cut() {
            cut = true;
            pipes.forEach(p -> p.silent = true); // in-flight connections go silent too
        }

        synchronized void heal() {
            if (!cut) return;
            cut = false;
            pipes.stream().filter(p -> p.silent).toList().forEach(Pipe::close);
        }

        void close() {
            try {
                server.close();
            } catch (IOException ignored) {
                // shutting down
            }
            pipes.forEach(Pipe::close);
        }

        private void acceptLoop() {
            while (!server.isClosed()) {
                try {
                    Socket client = server.accept();
                    client.setTcpNoDelay(true);
                    Pipe pipe = new Pipe(client);
                    synchronized (this) {
                        pipe.silent = cut;
                        pipes.add(pipe);
                    }
                    Thread.ofVirtual().start(() -> pipe.run(targetPort));
                } catch (IOException e) {
                    // closed, or one failed accept: keep going until the link is closed
                }
            }
        }

        /** One proxied connection. While {@code silent}, everything sent into it is discarded. */
        private final class Pipe {
            private final Socket client;
            private volatile Socket upstream;
            volatile boolean silent;

            Pipe(Socket client) {
                this.client = client;
            }

            void run(int targetPort) {
                try {
                    if (!silent) {
                        upstream = new Socket();
                        upstream.setTcpNoDelay(true);
                        upstream.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), targetPort), 1000);
                        Socket up = upstream;
                        Thread.ofVirtual().start(() -> copy(up, client)); // replies (raftis sends none, but be general)
                    }
                    copy(client, upstream);
                } catch (IOException e) {
                    // target down (killed node): drop the connection; the sender will retry
                } finally {
                    close();
                }
            }

            private void copy(Socket source, Socket target) {
                byte[] buf = new byte[64 * 1024];
                try (InputStream in = source.getInputStream()) {
                    OutputStream out = target == null ? null : target.getOutputStream();
                    int n;
                    while ((n = in.read(buf)) >= 0) {
                        if (silent || out == null) continue; // partitioned: the bytes vanish
                        out.write(buf, 0, n);
                        delivered.addAndGet(n);
                    }
                } catch (IOException e) {
                    // connection closed on one side
                } finally {
                    close();
                }
            }

            void close() {
                pipes.remove(this);
                closeQuietly(client);
                closeQuietly(upstream);
            }

            private static void closeQuietly(Socket s) {
                if (s == null) return;
                try {
                    s.close();
                } catch (IOException ignored) {
                    // already closed
                }
            }
        }
    }
}
