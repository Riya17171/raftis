package com.raftis.command;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/** Maps command names (case-insensitive) to their {@link CommandSpec}. */
public final class CommandTable {
    private final Map<String, CommandSpec> byName = new HashMap<>();

    public static CommandTable standard() {
        CommandTable table = new CommandTable();
        ServerCommands.register(table);
        KeyCommands.register(table);
        StringCommands.register(table);
        ListCommands.register(table);
        HashCommands.register(table);
        ZSetCommands.register(table);
        PubSubCommands.register(table);
        return table;
    }

    public void add(String name, int arity, int flags, CommandSpec.Handler handler) {
        byName.put(name, new CommandSpec(name, arity, flags, handler));
    }

    public CommandSpec lookup(byte[] name) {
        if (name.length > 64) return null;
        return byName.get(new String(name, StandardCharsets.ISO_8859_1).toLowerCase(Locale.ROOT));
    }

    public int size() {
        return byName.size();
    }
}
