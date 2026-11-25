package com.raftis.persistence;

import java.util.Locale;

/**
 * When the append-only file is forced to disk with fsync. Writing to a file only puts data in
 * the OS page cache; fsync is what makes it survive a power cut.
 */
public enum FsyncPolicy {
    /** fsync before replying to any write. Safest and slowest; group commit makes it affordable. */
    ALWAYS,
    /** fsync once per second in a background thread. A crash can lose about the last second of writes. */
    EVERYSEC,
    /** Never fsync; let the OS decide. Fastest and least safe. */
    NO;

    public static FsyncPolicy parse(String value) {
        return switch (value.toLowerCase(Locale.ROOT)) {
            case "always" -> ALWAYS;
            case "everysec" -> EVERYSEC;
            case "no" -> NO;
            default -> throw new IllegalArgumentException("appendfsync must be always, everysec or no");
        };
    }

    public String configName() {
        return name().toLowerCase(Locale.ROOT);
    }
}
