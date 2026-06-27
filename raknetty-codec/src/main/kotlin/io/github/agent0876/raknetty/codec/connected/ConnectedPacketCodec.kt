package io.github.agent0876.raknetty.codec.connected

import io.github.agent0876.raknetty.codec.connected.ConnectedPacket.Companion.INTERNAL_ADDRESS_COUNT
import io.github.agent0876.raknetty.codec.ext.UNASSIGNED_ADDRESS
import io.github.agent0876.raknetty.codec.ext.IPV6_ADDRESS_SIZE
import io.github.agent0876.raknetty.codec.ext.readAddress
import io.github.agent0876.raknetty.codec.ext.writeAddress
import io.github.agent0876.raknetty.core.exception.InvalidPacketException
import io.github.agent0876.raknetty.core.protocol.PacketId
import io.netty5.buffer.Buffer
import io.netty5.buffer.BufferAllocator

/**
 * Encode/decode logic for [ConnectedPacket]s.
 *
 * [decode] receives the frame payload starting at the ID byte.
 * [encode] writes the full packet (including ID byte) into [out].
 */
object ConnectedPacketCodec {

    // ── Decode ────────────────────────────────────────────────────────────────

    fun decode(buf: Buffer): ConnectedPacket {
        return when (val id = buf.readUnsignedByte().toInt()) {
            PacketId.CONNECTION_REQUEST          -> decodeConnectionRequest(buf)
            PacketId.CONNECTION_REQUEST_ACCEPTED -> decodeConnectionRequestAccepted(buf)
            PacketId.NEW_INCOMING_CONNECTION     -> decodeNewIncomingConnection(buf)
            PacketId.DISCONNECTION_NOTIFICATION  -> ConnectedPacket.DisconnectionNotification
            PacketId.DETECT_LOST_CONNECTION      -> ConnectedPacket.DetectLostConnection
            PacketId.CONNECTED_PING              -> ConnectedPacket.ConnectedPing(buf.readLong())
            PacketId.CONNECTED_PONG              -> ConnectedPacket.ConnectedPong(buf.readLong(), buf.readLong())
            else -> throw InvalidPacketException(
                "Unknown connected packet id: 0x${id.toString(16).padStart(2, '0')}"
            )
        }
    }

    private fun decodeConnectionRequest(buf: Buffer): ConnectedPacket.ConnectionRequest {
        val guid       = buf.readLong()
        val timestamp  = buf.readLong()
        val doSecurity = buf.readBoolean()
        return ConnectedPacket.ConnectionRequest(guid, timestamp, doSecurity)
    }

    private fun decodeConnectionRequestAccepted(buf: Buffer): ConnectedPacket.ConnectionRequestAccepted {
        val clientAddress = buf.readAddress()
        val clientIndex   = buf.readUnsignedShort()
        val addresses     = List(INTERNAL_ADDRESS_COUNT) { buf.readAddress() }
        val reqTs         = buf.readLong()
        val accTs         = buf.readLong()
        return ConnectedPacket.ConnectionRequestAccepted(clientAddress, clientIndex, addresses, reqTs, accTs)
    }

    private fun decodeNewIncomingConnection(buf: Buffer): ConnectedPacket.NewIncomingConnection {
        val serverAddress    = buf.readAddress()
        val internalAddresses = List(INTERNAL_ADDRESS_COUNT) { buf.readAddress() }
        val reqTs            = buf.readLong()
        val accTs            = buf.readLong()
        return ConnectedPacket.NewIncomingConnection(serverAddress, internalAddresses, reqTs, accTs)
    }

    // ── Encode ────────────────────────────────────────────────────────────────

    fun encode(packet: ConnectedPacket, alloc: BufferAllocator): Buffer {
        // Pre-size to the worst-case IPv6 payload to avoid buffer reallocation.
        // ConnectionRequestAccepted: 1 + 29 + 2 + 10×29 + 16 = 338B
        // NewIncomingConnection:     1 + 29      + 10×29 + 16 = 336B
        val initialSize = when (packet) {
            is ConnectedPacket.ConnectionRequestAccepted ->
                1 + IPV6_ADDRESS_SIZE + 2 + INTERNAL_ADDRESS_COUNT * IPV6_ADDRESS_SIZE + 16
            is ConnectedPacket.NewIncomingConnection ->
                1 + IPV6_ADDRESS_SIZE + INTERNAL_ADDRESS_COUNT * IPV6_ADDRESS_SIZE + 16
            else -> 64
        }
        val buf = alloc.allocate(initialSize)
        when (packet) {
            is ConnectedPacket.ConnectionRequest -> {
                buf.writeByte(PacketId.CONNECTION_REQUEST.toByte())
                buf.writeLong(packet.clientGuid)
                buf.writeLong(packet.requestTimestamp)
                buf.writeBoolean(packet.doSecurity)
            }
            is ConnectedPacket.ConnectionRequestAccepted -> {
                buf.writeByte(PacketId.CONNECTION_REQUEST_ACCEPTED.toByte())
                buf.writeAddress(packet.clientAddress)
                buf.writeShort(packet.clientIndex.toShort())
                val addresses = packet.systemAddresses
                repeat(INTERNAL_ADDRESS_COUNT) { i ->
                    buf.writeAddress(addresses.getOrElse(i) { UNASSIGNED_ADDRESS })
                }
                buf.writeLong(packet.requestTimestamp)
                buf.writeLong(packet.acceptedTimestamp)
            }
            is ConnectedPacket.NewIncomingConnection -> {
                buf.writeByte(PacketId.NEW_INCOMING_CONNECTION.toByte())
                buf.writeAddress(packet.serverAddress)
                val addresses = packet.internalAddresses
                repeat(INTERNAL_ADDRESS_COUNT) { i ->
                    buf.writeAddress(addresses.getOrElse(i) { UNASSIGNED_ADDRESS })
                }
                buf.writeLong(packet.requestTimestamp)
                buf.writeLong(packet.acceptedTimestamp)
            }
            ConnectedPacket.DisconnectionNotification ->
                buf.writeByte(PacketId.DISCONNECTION_NOTIFICATION.toByte())
            ConnectedPacket.DetectLostConnection ->
                buf.writeByte(PacketId.DETECT_LOST_CONNECTION.toByte())
            is ConnectedPacket.ConnectedPing -> {
                buf.writeByte(PacketId.CONNECTED_PING.toByte())
                buf.writeLong(packet.sendTimestamp)
            }
            is ConnectedPacket.ConnectedPong -> {
                buf.writeByte(PacketId.CONNECTED_PONG.toByte())
                buf.writeLong(packet.sendTimestamp)
                buf.writeLong(packet.pingTimestamp)
            }
        }
        return buf
    }
}
