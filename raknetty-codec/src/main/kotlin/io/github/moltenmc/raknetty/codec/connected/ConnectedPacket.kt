package io.github.moltenmc.raknetty.codec.connected

import java.net.InetSocketAddress

/**
 * Connected-mode RakNet messages carried inside [RakNetFrame] payloads.
 *
 * These are decoded from the raw bytes of a fully reassembled, ordered frame
 * by [ConnectedPacketCodec]. The first byte is the message ID.
 */
sealed class ConnectedPacket {

    data class ConnectionRequest(
        val clientGuid: Long,
        val requestTimestamp: Long,
        val doSecurity: Boolean,
    ) : ConnectedPacket()

    /**
     * Sent by the server; contains the client's external address and a set of
     * the server's known internal addresses (up to [INTERNAL_ADDRESS_COUNT]).
     */
    data class ConnectionRequestAccepted(
        val clientAddress: InetSocketAddress,
        val clientIndex: Int,               // system index, usually 0
        val systemAddresses: List<InetSocketAddress>,
        val requestTimestamp: Long,
        val acceptedTimestamp: Long,
    ) : ConnectedPacket()

    data class NewIncomingConnection(
        val serverAddress: InetSocketAddress,
        val internalAddresses: List<InetSocketAddress>,
        val requestTimestamp: Long,
        val acceptedTimestamp: Long,
    ) : ConnectedPacket()

    data object DisconnectionNotification : ConnectedPacket()
    data object DetectLostConnection      : ConnectedPacket()

    data class ConnectedPing(val sendTimestamp: Long) : ConnectedPacket()
    data class ConnectedPong(val sendTimestamp: Long, val pingTimestamp: Long) : ConnectedPacket()

    companion object {
        /** Number of SystemAddresses in ConnectionRequestAccepted / NewIncomingConnection. */
        const val INTERNAL_ADDRESS_COUNT = 10
    }
}
