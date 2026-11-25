package com.raftis.server;

import com.raftis.store.ByteKey;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/** Channel subscriptions. Publishing appends the message to each subscriber's output buffer. */
public final class PubSub {
    private static final byte[] MESSAGE = "message".getBytes(StandardCharsets.US_ASCII);

    private final Map<ByteKey, Set<Connection>> channels = new HashMap<>();
    private final Consumer<Connection> onOutput;

    PubSub(Consumer<Connection> onOutput) {
        this.onOutput = onOutput;
    }

    /** Returns how many channels the client is now subscribed to. */
    public int subscribe(Connection c, ByteKey channel) {
        if (c.mutableSubscriptions().add(channel)) {
            channels.computeIfAbsent(channel, k -> new LinkedHashSet<>()).add(c);
        }
        return c.subscriptions().size();
    }

    public void unsubscribe(Connection c, ByteKey channel) {
        if (!c.mutableSubscriptions().remove(channel)) return;
        Set<Connection> subscribers = channels.get(channel);
        if (subscribers != null) {
            subscribers.remove(c);
            if (subscribers.isEmpty()) channels.remove(channel);
        }
    }

    void unsubscribeAll(Connection c) {
        for (ByteKey channel : Set.copyOf(c.subscriptions())) unsubscribe(c, channel);
    }

    /** Returns how many clients received the message. */
    public int publish(ByteKey channel, byte[] message) {
        Set<Connection> subscribers = channels.get(channel);
        if (subscribers == null) return 0;
        for (Connection c : subscribers) {
            c.out.arrayHeader(3).bulk(MESSAGE).bulk(channel.bytes()).bulk(message);
            onOutput.accept(c);
        }
        return subscribers.size();
    }

    public int channelCount() {
        return channels.size();
    }
}
