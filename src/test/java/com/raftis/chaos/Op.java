package com.raftis.chaos;

/**
 * One client operation on one key, as the client saw it.
 *
 * @param value       WRITE: the value written. READ: the value read (null = the key didn't exist).
 *                    INCR: the new value returned (null when the outcome is unknown).
 * @param invokedAt   nanoseconds since the test started, just before the request was sent
 * @param completedAt nanoseconds since the test started, just after the reply arrived
 *                    (for an UNKNOWN outcome: when the client gave up waiting)
 */
record Op(int client, String key, Type type, Long value, long invokedAt, long completedAt, Outcome outcome) {

    enum Type { READ, WRITE, INCR }

    /** OK: the server answered. UNKNOWN: no answer (timeout, crash, UNCERTAIN), so it may or may not have happened. */
    enum Outcome { OK, UNKNOWN }

    @Override
    public String toString() {
        String what = switch (type) {
            case READ -> "read  " + key + " -> " + (value == null ? "(nil)" : value);
            case WRITE -> "write " + key + " = " + value;
            case INCR -> "incr  " + key + " -> " + (value == null ? "?" : value);
        };
        String end = outcome == Outcome.OK ? String.format("%.3fs", completedAt / 1e9) : "never answered";
        return String.format("client %-2d %-28s [%.3fs .. %s]%s", client, what, invokedAt / 1e9, end,
                outcome == Outcome.UNKNOWN ? "  (outcome unknown)" : "");
    }
}
