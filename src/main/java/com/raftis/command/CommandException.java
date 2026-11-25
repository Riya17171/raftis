package com.raftis.command;

/**
 * Thrown by a command handler to reply with an error. The message starts with an error code,
 * using Redis's exact wording so client libraries behave the same as with real Redis.
 */
public final class CommandException extends RuntimeException {
    public static final String WRONGTYPE = "WRONGTYPE Operation against a key holding the wrong kind of value";

    public CommandException(String message) {
        super(message, null, false, false); // no stack trace: these are normal replies, not bugs
    }

    public static CommandException wrongType() {
        return new CommandException(WRONGTYPE);
    }

    public static CommandException syntax() {
        return new CommandException("ERR syntax error");
    }

    public static CommandException notInteger() {
        return new CommandException("ERR value is not an integer or out of range");
    }

    public static CommandException notFloat() {
        return new CommandException("ERR value is not a valid float");
    }

    public static CommandException wrongArgs(String command) {
        return new CommandException("ERR wrong number of arguments for '" + command + "' command");
    }

    public static CommandException overflow() {
        return new CommandException("ERR increment or decrement would overflow");
    }
}
