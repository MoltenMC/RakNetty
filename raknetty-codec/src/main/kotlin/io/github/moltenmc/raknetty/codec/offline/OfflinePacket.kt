package io.github.moltenmc.raknetty.codec.offline

import java.net.InetSocketAddress

/**
 * Unconnected (offline) RakNet messages — exchanged before a connection is established.
 * Identified by the absence of [FLAG_VALID] (0x80) in the first byte.
 */
sealed class OfflinePacket {

    data class UnconnectedPing(
        val sendTimestamp: Long,
        val senderGuid: Long,
    ) : OfflinePacket()

    data class UnconnectedPong(
        val sendTimestamp: Long,
        val serverGuid: Long,
        val serverInfo: String,     // MOTD or game-specific advertisement string
    ) : OfflinePacket()

    /**
     * Sent by client to probe the server at each MTU size.
     * [mtu] is inferred from the total datagram payload size + 28 (IP + UDP overhead).
     */
    data class OpenConnectionRequest1(
        val protocolVersion: Int,
        val mtu: Int,
    ) : OfflinePacket()

    data class OpenConnectionReply1(
        val serverGuid: Long,
        val useSecurity: Boolean,
        val mtu: Int,
    ) : OfflinePacket()

    data class OpenConnectionRequest2(
        val serverAddress: InetSocketAddress,
        val mtu: Int,
        val clientGuid: Long,
    ) : OfflinePacket()

    data class OpenConnectionReply2(
        val serverGuid: Long,
        val clientAddress: InetSocketAddress,
        val mtu: Int,
        val encryptionEnabled: Boolean,
    ) : OfflinePacket()

    data class IncompatibleProtocolVersion(
        val protocolVersion: Int,
        val serverGuid: Long,
    ) : OfflinePacket()

    data class AlreadyConnected(val serverGuid: Long) : OfflinePacket()
    data class NoFreeIncomingConnections(val serverGuid: Long) : OfflinePacket()
    data class IpRecentlyConnected(val serverGuid: Long) : OfflinePacket()
    data class ConnectionBanned(val serverGuid: Long) : OfflinePacket()
}
