package io.github.agent0876.raknetty.codec.offline

import io.github.agent0876.raknetty.codec.ext.readAddress
import io.github.agent0876.raknetty.codec.ext.readMagic
import io.github.agent0876.raknetty.codec.ext.writeAddress
import io.github.agent0876.raknetty.codec.ext.writeMagic
import io.github.agent0876.raknetty.core.exception.InvalidPacketException
import io.github.agent0876.raknetty.core.protocol.PacketId
import io.netty5.buffer.Buffer
import io.netty5.buffer.BufferAllocator
import java.nio.ByteBuffer

/**
 * Pure encode/decode logic for [OfflinePacket]s.
 * Does not implement [io.netty5.channel.ChannelHandler] — use from a handler.
 *
 * [decode] receives the **full** datagram payload (ID byte not yet consumed),
 * which is required to compute the MTU for [OfflinePacket.OpenConnectionRequest1].
 *
 * Returns `null` when the magic bytes are invalid (non-RakNet traffic); the
 * caller should silently drop the datagram in that case.
 */
object OfflinePacketCodec {

    // ── Decode ────────────────────────────────────────────────────────────────

    fun decode(buf: Buffer): OfflinePacket? {
        val totalPayloadSize = buf.readableBytes()
        return when (val id = buf.readUnsignedByte().toInt()) {
            PacketId.UNCONNECTED_PING,
            PacketId.UNCONNECTED_PING_OPEN_CONNECTIONS -> decodePing(buf)

            PacketId.UNCONNECTED_PONG              -> decodePong(buf)
            PacketId.OPEN_CONNECTION_REQUEST_1     -> decodeOCReq1(buf, totalPayloadSize)
            PacketId.OPEN_CONNECTION_REPLY_1       -> decodeOCReply1(buf)
            PacketId.OPEN_CONNECTION_REQUEST_2     -> decodeOCReq2(buf)
            PacketId.OPEN_CONNECTION_REPLY_2       -> decodeOCReply2(buf)
            PacketId.INCOMPATIBLE_PROTOCOL_VERSION -> decodeIncompatible(buf)
            PacketId.ALREADY_CONNECTED             -> { if (!buf.readMagic()) return null; OfflinePacket.AlreadyConnected(buf.readLong()) }
            PacketId.NO_FREE_INCOMING_CONNECTIONS  -> { if (!buf.readMagic()) return null; OfflinePacket.NoFreeIncomingConnections(buf.readLong()) }
            PacketId.IP_RECENTLY_CONNECTED         -> { if (!buf.readMagic()) return null; OfflinePacket.IpRecentlyConnected(buf.readLong()) }
            PacketId.CONNECTION_BANNED             -> { if (!buf.readMagic()) return null; OfflinePacket.ConnectionBanned(buf.readLong()) }

            else -> throw InvalidPacketException("Unknown offline packet id: 0x${id.toString(16)}")
        }
    }

    private fun decodePing(buf: Buffer): OfflinePacket.UnconnectedPing? {
        val ts = buf.readLong()
        if (!buf.readMagic()) return null
        val guid = buf.readLong()
        return OfflinePacket.UnconnectedPing(ts, guid)
    }

    private fun decodePong(buf: Buffer): OfflinePacket.UnconnectedPong? {
        val ts = buf.readLong()
        val guid = buf.readLong()
        if (!buf.readMagic()) return null
        val infoLen = buf.readUnsignedShort()
        val info = if (buf.readableBytes() >= infoLen) {
            buf.readCharSequence(infoLen, Charsets.UTF_8).toString()
        } else ""
        return OfflinePacket.UnconnectedPong(ts, guid, info)
    }

    private fun decodeOCReq1(buf: Buffer, totalPayloadSize: Int): OfflinePacket.OpenConnectionRequest1? {
        if (!buf.readMagic()) return null
        val protocolVersion = buf.readUnsignedByte().toInt()
        // remaining bytes are zero-padding to reach MTU; MTU = total payload + IP(20) + UDP(8)
        val mtu = totalPayloadSize + 28
        return OfflinePacket.OpenConnectionRequest1(protocolVersion, mtu)
    }

    private fun decodeOCReply1(buf: Buffer): OfflinePacket.OpenConnectionReply1? {
        if (!buf.readMagic()) return null
        val guid = buf.readLong()
        val security = buf.readBoolean()
        val mtu = buf.readUnsignedShort()
        return OfflinePacket.OpenConnectionReply1(guid, security, mtu)
    }

    private fun decodeOCReq2(buf: Buffer): OfflinePacket.OpenConnectionRequest2? {
        if (!buf.readMagic()) return null
        val serverAddr = buf.readAddress()
        val mtu = buf.readUnsignedShort()
        val clientGuid = buf.readLong()
        return OfflinePacket.OpenConnectionRequest2(serverAddr, mtu, clientGuid)
    }

    private fun decodeOCReply2(buf: Buffer): OfflinePacket.OpenConnectionReply2? {
        if (!buf.readMagic()) return null
        val serverGuid = buf.readLong()
        val clientAddr = buf.readAddress()
        val mtu = buf.readUnsignedShort()
        val encryption = buf.readBoolean()
        return OfflinePacket.OpenConnectionReply2(serverGuid, clientAddr, mtu, encryption)
    }

    private fun decodeIncompatible(buf: Buffer): OfflinePacket.IncompatibleProtocolVersion? {
        val proto = buf.readUnsignedByte().toInt()
        if (!buf.readMagic()) return null
        val guid = buf.readLong()
        return OfflinePacket.IncompatibleProtocolVersion(proto, guid)
    }

    // ── Encode ────────────────────────────────────────────────────────────────

    fun encode(packet: OfflinePacket, alloc: BufferAllocator): Buffer {
        val buf = alloc.allocate(2048)
        when (packet) {
            is OfflinePacket.UnconnectedPing -> {
                buf.writeByte(PacketId.UNCONNECTED_PING.toByte())
                buf.writeLong(packet.sendTimestamp)
                buf.writeMagic()
                buf.writeLong(packet.senderGuid)
            }
            is OfflinePacket.UnconnectedPong -> {
                buf.writeByte(PacketId.UNCONNECTED_PONG.toByte())
                buf.writeLong(packet.sendTimestamp)
                buf.writeLong(packet.serverGuid)
                buf.writeMagic()
                val infoBytes = packet.serverInfo.toByteArray(Charsets.UTF_8)
                buf.writeShort(infoBytes.size.toShort())
                buf.writeBytes(ByteBuffer.wrap(infoBytes))
            }
            is OfflinePacket.OpenConnectionRequest1 -> {
                val payloadSize = packet.mtu - 28   // strip IP + UDP headers
                buf.writeByte(PacketId.OPEN_CONNECTION_REQUEST_1.toByte())
                buf.writeMagic()
                buf.writeByte(packet.protocolVersion.toByte())
                val paddingLen = payloadSize - 1 - 16 - 1  // id + magic + version
                if (paddingLen > 0) buf.writeBytes(ByteBuffer.allocate(paddingLen))
            }
            is OfflinePacket.OpenConnectionReply1 -> {
                buf.writeByte(PacketId.OPEN_CONNECTION_REPLY_1.toByte())
                buf.writeMagic()
                buf.writeLong(packet.serverGuid)
                buf.writeBoolean(packet.useSecurity)
                buf.writeShort(packet.mtu.toShort())
            }
            is OfflinePacket.OpenConnectionRequest2 -> {
                buf.writeByte(PacketId.OPEN_CONNECTION_REQUEST_2.toByte())
                buf.writeMagic()
                buf.writeAddress(packet.serverAddress)
                buf.writeShort(packet.mtu.toShort())
                buf.writeLong(packet.clientGuid)
            }
            is OfflinePacket.OpenConnectionReply2 -> {
                buf.writeByte(PacketId.OPEN_CONNECTION_REPLY_2.toByte())
                buf.writeMagic()
                buf.writeLong(packet.serverGuid)
                buf.writeAddress(packet.clientAddress)
                buf.writeShort(packet.mtu.toShort())
                buf.writeBoolean(packet.encryptionEnabled)
            }
            is OfflinePacket.IncompatibleProtocolVersion -> {
                buf.writeByte(PacketId.INCOMPATIBLE_PROTOCOL_VERSION.toByte())
                buf.writeByte(packet.protocolVersion.toByte())
                buf.writeMagic()
                buf.writeLong(packet.serverGuid)
            }
            is OfflinePacket.AlreadyConnected -> {
                buf.writeByte(PacketId.ALREADY_CONNECTED.toByte()); buf.writeMagic(); buf.writeLong(packet.serverGuid)
            }
            is OfflinePacket.NoFreeIncomingConnections -> {
                buf.writeByte(PacketId.NO_FREE_INCOMING_CONNECTIONS.toByte()); buf.writeMagic(); buf.writeLong(packet.serverGuid)
            }
            is OfflinePacket.IpRecentlyConnected -> {
                buf.writeByte(PacketId.IP_RECENTLY_CONNECTED.toByte()); buf.writeMagic(); buf.writeLong(packet.serverGuid)
            }
            is OfflinePacket.ConnectionBanned -> {
                buf.writeByte(PacketId.CONNECTION_BANNED.toByte()); buf.writeMagic(); buf.writeLong(packet.serverGuid)
            }
        }
        return buf
    }
}
