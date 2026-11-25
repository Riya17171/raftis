package com.raftis.store;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GlobTest {
    private static boolean m(String pattern, String s) {
        return Glob.match(pattern.getBytes(StandardCharsets.UTF_8), s.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void matchesLikeRedis() {
        assertTrue(m("*", "anything"));
        assertTrue(m("user:*", "user:42"));
        assertFalse(m("user:*", "session:42"));
        assertTrue(m("h?llo", "hello"));
        assertFalse(m("h?llo", "hllo"));
        assertTrue(m("h[ae]llo", "hallo"));
        assertFalse(m("h[^e]llo", "hello"));
        assertTrue(m("h[a-c]llo", "hbllo"));
        assertTrue(m("a\\*b", "a*b"));
        assertFalse(m("a\\*b", "axb"));
        assertTrue(m("*:*:end", "a:b:end"));
    }
}
