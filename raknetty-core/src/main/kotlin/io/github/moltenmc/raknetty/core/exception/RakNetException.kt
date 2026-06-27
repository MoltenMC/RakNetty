package io.github.moltenmc.raknetty.core.exception

import io.github.moltenmc.raknetty.core.connection.DisconnectReason

open class RakNetException(message: String, cause: Throwable? = null) : Exception(message, cause)

/** Thrown when the Open Connection or post-connection handshake fails. */
class HandshakeException(message: String) : RakNetException(message)

/** Thrown when a received datagram cannot be decoded (wrong magic, truncated, etc.). */
class InvalidPacketException(message: String) : RakNetException(message)

/** Thrown when an operation is attempted on an already-closed connection. */
class ConnectionClosedException(val reason: DisconnectReason)
    : RakNetException("Connection closed: $reason")
