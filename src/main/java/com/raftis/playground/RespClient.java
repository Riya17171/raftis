package com.raftis.playground;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/** A minimal blocking Redis client: enough for the playground's traffic generator. */
final class RespClient implements AutoCloseable {
    /** An error reply from the server, such as {@code NOTLEADER 127.0.0.1:6381}. */
    static final class ErrorReply extends Exception {
        ErrorReply(String message) {
            super(message, null, false, false);
        }
    }

    private final Socket socket;
    private final InputStream in;
    private final OutputStream out;

    RespClient(int port, int timeoutMs) throws IOException {
        socket = new Socket();
        socket.setTcpNoDelay(true);
        socket.setSoTimeout(timeoutMs);
        socket.connect(new InetSocketAddress("127.0.0.1", port), timeoutMs);
        in = new BufferedInputStream(socket.getInputStream());
        out = socket.getOutputStream();
    }

    /** Sends a command; returns a String, a Long, or null (nil). Throws ErrorReply for "-ERR ..." replies. */
    Object call(String... args) throws IOException, ErrorReply {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        b.writeBytes(("*" + args.length + "\r\n").getBytes(StandardCharsets.US_ASCII));
        for (String a : args) {
            byte[] bytes = a.getBytes(StandardCharsets.UTF_8);
            b.writeBytes(("$" + bytes.length + "\r\n").getBytes(StandardCharsets.US_ASCII));
            b.writeBytes(bytes);
            b.writeBytes(new byte[] {'\r', '\n'});
        }
        out.write(b.toByteArray());
        out.flush();
        return readReply();
    }

    private Object readReply() throws IOException, ErrorReply {
        int type = in.read();
        String line = readLine();
        return switch (type) {
            case '+' -> line;
            case '-' -> throw new ErrorReply(line);
            case ':' -> Long.parseLong(line);
            case '$' -> {
                int n = Integer.parseInt(line);
                if (n < 0) yield null;
                byte[] data = in.readNBytes(n + 2);
                if (data.length < n + 2) throw new IOException("connection closed");
                yield new String(data, 0, n, StandardCharsets.UTF_8);
            }
            case -1 -> throw new IOException("connection closed");
            default -> throw new IOException("unexpected reply type " + (char) type);
        };
    }

    private String readLine() throws IOException {
        StringBuilder sb = new StringBuilder();
        int c;
        while ((c = in.read()) != -1) {
            if (c == '\r') {
                in.read(); // '\n'
                return sb.toString();
            }
            sb.append((char) c);
        }
        throw new IOException("connection closed");
    }

    @Override
    public void close() {
        try {
            socket.close();
        } catch (IOException ignored) {
            // already closed
        }
    }
}
