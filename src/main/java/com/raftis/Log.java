package com.raftis;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

/** Tiny Redis-style logger: {@code 4242:M 25 Nov 2026 22:40:01.123 * message}. */
public final class Log {
    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("dd MMM yyyy HH:mm:ss.SSS", Locale.ENGLISH);
    private static final long PID = ProcessHandle.current().pid();

    private Log() {}

    public static void info(String format, Object... args) {
        print('*', format, args);
    }

    public static void warn(String format, Object... args) {
        print('#', format, args);
    }

    private static synchronized void print(char level, String format, Object... args) {
        System.out.println(PID + ":M " + LocalDateTime.now().format(TIME) + " " + level + " " + String.format(format, args));
    }
}
