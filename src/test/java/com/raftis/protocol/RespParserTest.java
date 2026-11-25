package com.raftis.protocol;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RespParserTest {

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static List<String> strings(List<byte[]> args) {
        List<String> out = new ArrayList<>();
        for (byte[] a : args) out.add(new String(a, StandardCharsets.UTF_8));
        return out;
    }

    @Test
    void parsesACompleteCommand() {
        byte[] in = b("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nhello\r\n");
        RespParser.Result r = RespParser.parse(in, 0, in.length);
        assertNotNull(r);
        assertEquals(List.of("SET", "key", "hello"), strings(r.args()));
        assertEquals(in.length, r.next());
    }

    /**
     * TCP can split a command anywhere. Cut it at every possible byte: the parser must wait
     * (return null) until the last byte arrives. The value contains \r\n to prove the parser
     * uses the length prefix, not line endings (binary safety).
     */
    @Test
    void waitsForTheWholeCommandWhereverItIsSplit() {
        byte[] in = b("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$7\r\nhe\r\nllo\r\n");
        for (int cut = 0; cut < in.length; cut++) {
            assertNull(RespParser.parse(in, 0, cut), "should wait for more bytes when cut at " + cut);
        }
        assertEquals(List.of("SET", "key", "he\r\nllo"), strings(RespParser.parse(in, 0, in.length).args()));
    }

    @Test
    void parsesPipelinedCommandsOneAtATime() {
        byte[] in = b("*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n$1\r\na\r\n*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\n");
        List<List<String>> commands = new ArrayList<>();
        int pos = 0;
        RespParser.Result r;
        while ((r = RespParser.parse(in, pos, in.length)) != null) {
            commands.add(strings(r.args()));
            pos = r.next();
        }
        assertEquals(List.of(List.of("PING"), List.of("GET", "a"), List.of("ECHO", "hi")), commands);
        assertEquals(in.length, pos);
    }

    @Test
    void parsesInlineCommands() {
        byte[] in = b("PING\r\nSET  a\tb\n");
        RespParser.Result first = RespParser.parse(in, 0, in.length);
        assertEquals(List.of("PING"), strings(first.args()));
        RespParser.Result second = RespParser.parse(in, first.next(), in.length);
        assertEquals(List.of("SET", "a", "b"), strings(second.args()));
    }

    @Test
    void emptyArrayIsANoOp() {
        RespParser.Result r = RespParser.parse(b("*0\r\n"), 0, 4);
        assertTrue(r.args().isEmpty());
        assertEquals(4, r.next());
    }

    @Test
    void rejectsMalformedInput() {
        assertThrows(ProtocolException.class, () -> parse("*1\r\n:5\r\n"));
        assertThrows(ProtocolException.class, () -> parse("*abc\r\n"));
        assertThrows(ProtocolException.class, () -> parse("*1\r\n$-5\r\n"));
        assertThrows(ProtocolException.class, () -> parse("*1\r\n$3\r\nabcXY"));
    }

    private static void parse(String s) {
        byte[] in = b(s);
        RespParser.parse(in, 0, in.length);
    }
}
