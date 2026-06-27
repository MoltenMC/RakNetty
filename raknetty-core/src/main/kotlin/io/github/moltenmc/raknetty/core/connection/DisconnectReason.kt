package io.github.moltenmc.raknetty.core.connection

enum class DisconnectReason {
    CLIENT_REQUESTED,
    SERVER_REQUESTED,
    TIMED_OUT,
    BANNED,
    NO_FREE_CONNECTIONS,
    ALREADY_CONNECTED,
    INVALID_PASSWORD,
    INCOMPATIBLE_PROTOCOL,
    INTERNAL_ERROR,
}
