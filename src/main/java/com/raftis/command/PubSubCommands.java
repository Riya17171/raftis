package com.raftis.command;

import com.raftis.server.Connection;
import com.raftis.server.PubSub;
import com.raftis.store.ByteKey;

import java.util.ArrayList;
import java.util.List;

import static com.raftis.command.CommandSpec.PUBSUB;

/** SUBSCRIBE, UNSUBSCRIBE, PUBLISH. Messages aren't stored, so these never go to the log. */
final class PubSubCommands {
    private PubSubCommands() {}

    static void register(CommandTable t) {
        t.add("subscribe", -2, PUBSUB, PubSubCommands::subscribe);
        t.add("unsubscribe", -1, PUBSUB, PubSubCommands::unsubscribe);
        t.add("publish", 3, 0, PubSubCommands::publish);
    }

    private static void subscribe(Context ctx, List<byte[]> a) {
        Connection c = ctx.conn();
        PubSub pubsub = ctx.server().pubsub();
        for (int i = 1; i < a.size(); i++) {
            ByteKey channel = Args.key(a, i);
            int count = pubsub.subscribe(c, channel);
            ctx.out.arrayHeader(3).bulk("subscribe").bulk(channel.bytes()).integer(count);
        }
    }

    private static void unsubscribe(Context ctx, List<byte[]> a) {
        Connection c = ctx.conn();
        PubSub pubsub = ctx.server().pubsub();
        List<ByteKey> channels = new ArrayList<>();
        if (a.size() > 1) {
            for (int i = 1; i < a.size(); i++) channels.add(Args.key(a, i));
        } else {
            channels.addAll(c.subscriptions());
        }
        if (channels.isEmpty()) {
            ctx.out.arrayHeader(3).bulk("unsubscribe").nullBulk().integer(0);
            return;
        }
        for (ByteKey channel : channels) {
            pubsub.unsubscribe(c, channel);
            ctx.out.arrayHeader(3).bulk("unsubscribe").bulk(channel.bytes()).integer(c.subscriptions().size());
        }
    }

    private static void publish(Context ctx, List<byte[]> a) {
        ctx.out.integer(ctx.server().pubsub().publish(Args.key(a, 1), a.get(2)));
    }
}
