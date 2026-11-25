package com.raftis.command;

import java.util.List;

/**
 * One command: its name, how many arguments it takes, its flags, and the code that runs it.
 *
 * @param arity like Redis: a positive number means exactly that many words (including the
 *              command name); a negative number -N means at least N words
 */
public record CommandSpec(String name, int arity, int flags, Handler handler) {
    /** Changes the dataset. Only these go to the append-only file (and, later, through Raft). */
    public static final int WRITE = 1;
    /** Allowed while the client is subscribed to pub/sub channels. */
    public static final int PUBSUB = 1 << 1;

    @FunctionalInterface
    public interface Handler {
        void run(Context ctx, List<byte[]> args);
    }

    public boolean arityMatches(int argCount) {
        return arity >= 0 ? argCount == arity : argCount >= -arity;
    }

    public boolean has(int flag) {
        return (flags & flag) != 0;
    }
}
