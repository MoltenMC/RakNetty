package io.github.agent0876.raknetty.core.connection

enum class ConnectionState {
    UNCONNECTED,

    /** OCReq1/2 ↔ OCReply1/2 exchange in progress. */
    CONNECTING,

    /** ConnectionRequest ↔ ConnectionRequestAccepted exchange in progress. */
    HANDSHAKING,

    CONNECTED,
    DISCONNECTING,
    DISCONNECTED;

    val isActive: Boolean
        get() = this == CONNECTING || this == HANDSHAKING || this == CONNECTED
}
