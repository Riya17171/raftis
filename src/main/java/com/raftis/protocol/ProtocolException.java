package com.raftis.protocol;

/** The client sent bytes that are not valid RESP. The server replies with an error and closes the connection. */
public final class ProtocolException extends RuntimeException {
    public ProtocolException(String message) {
        super(message, null, false, false);
    }
}
