package com.raftis.playground;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntSupplier;

/**
 * Client traffic for the playground: a few threads writing and reading random keys on the
 * leader, plus one thread that INCRs a counter and keeps checking that no acknowledged
 * increment has been lost. Under Raft, after INCR returned n, every later read must see at least
 * n, whatever happened to the leader in between.
 */
final class Workload {
    private static final int TIMEOUT_MS = 1000;

    private final IntSupplier leaderPort; // the current leader's client port, or -1
    private final AtomicLong ok = new AtomicLong();
    private final AtomicLong errors = new AtomicLong();
    private final AtomicLong acked = new AtomicLong();       // highest INCR result acknowledged
    private final AtomicLong lost = new AtomicLong();        // acknowledged increments later found missing
    private final AtomicLong checks = new AtomicLong();      // reads that confirmed every acknowledged INCR
    private volatile String lastError = "";
    private volatile boolean on = true;
    private volatile boolean stopping;

    Workload(IntSupplier leaderPort) {
        this.leaderPort = leaderPort;
    }

    void start() {
        Thread.ofPlatform().daemon().name("traffic-counter").start(this::counterLoop);
        for (int i = 0; i < 3; i++) {
            long seed = i;
            Thread.ofPlatform().daemon().name("traffic-" + i).start(() -> mixedLoop(new Random(seed)));
        }
    }

    void setOn(boolean on) {
        this.on = on;
    }

    boolean isOn() {
        return on;
    }

    void stop() {
        stopping = true;
    }

    long ok() {
        return ok.get();
    }

    long errors() {
        return errors.get();
    }

    long acked() {
        return acked.get();
    }

    long checks() {
        return checks.get();
    }

    long lost() {
        return lost.get();
    }

    String lastError() {
        return lastError;
    }

    private void counterLoop() {
        RespClient c = null;
        int connectedTo = -1;
        long sinceCheck = 0;
        while (!stopping) {
            try {
                if (!on) {
                    Thread.sleep(100);
                    continue;
                }
                int port = leaderPort.getAsInt();
                if (port < 0) {
                    Thread.sleep(20);
                    continue;
                }
                if (c == null || port != connectedTo) {
                    if (c != null) c.close();
                    c = new RespClient(port, TIMEOUT_MS);
                    connectedTo = port;
                }
                if (++sinceCheck % 10 == 0) {
                    // Durability check: a linearizable read must see every acknowledged increment.
                    long mustSee = acked.get();
                    Object v = c.call("GET", "counter");
                    long seen = v == null ? 0 : Long.parseLong((String) v);
                    if (seen < mustSee) {
                        lost.addAndGet(mustSee - seen);
                        lastError = "LOST WRITES: read " + seen + " after " + mustSee + " was acknowledged";
                    } else {
                        checks.incrementAndGet();
                    }
                } else {
                    long n = (Long) c.call("INCR", "counter");
                    acked.accumulateAndGet(n, Math::max);
                }
                ok.incrementAndGet();
            } catch (RespClient.ErrorReply e) {
                failed(e.getMessage());
            } catch (IOException e) {
                failed("no reply: " + e.getMessage());
                if (c != null) c.close();
                c = null;
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    private void mixedLoop(Random rnd) {
        RespClient c = null;
        int connectedTo = -1;
        while (!stopping) {
            try {
                if (!on) {
                    Thread.sleep(100);
                    continue;
                }
                int port = leaderPort.getAsInt();
                if (port < 0) {
                    Thread.sleep(20);
                    continue;
                }
                if (c == null || port != connectedTo) {
                    if (c != null) c.close();
                    c = new RespClient(port, TIMEOUT_MS);
                    connectedTo = port;
                }
                String key = "key:" + rnd.nextInt(1000);
                if (rnd.nextBoolean()) c.call("SET", key, Long.toString(rnd.nextLong()));
                else c.call("GET", key);
                ok.incrementAndGet();
            } catch (RespClient.ErrorReply e) {
                failed(e.getMessage());
            } catch (IOException e) {
                failed("no reply: " + e.getMessage());
                if (c != null) c.close();
                c = null;
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    private void failed(String message) {
        errors.incrementAndGet();
        if (!message.startsWith("LOST")) lastError = message;
        try {
            Thread.sleep(50); // don't spin while the cluster is electing a leader
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
