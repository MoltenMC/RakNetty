package io.github.agent0876.raknetty.codec.ext

import io.github.agent0876.raknetty.core.exception.InvalidPacketException
import io.github.agent0876.raknetty.core.protocol.PacketId
import io.netty.buffer.ByteBuf
import java.net.Inet4Address
import java.net.Inet6Address
import java.net.InetSocketAddress

// ── Magic ────────────────────────────────────────────────────────────────────

fun ByteBuf.writeMagic() {
    writeBytes(PacketId.OFFLINE_MESSAGE_ID)
}

/**
 * Reads 16 bytes and returns `true` if they match [PacketId.OFFLINE_MESSAGE_ID].
 * Returns `false` for non-RakNet traffic so callers can drop the packet silently.
 */
fun ByteBuf.readMagic(): Boolean {
    val bytes = ByteArray(16)
    readBytes(bytes)
    return bytes.contentEquals(PacketId.OFFLINE_MESSAGE_ID)
}

fun ByteBuf.verifyMagic() {
    if (!readMagic()) throw InvalidPacketException("Offline message magic mismatch")
}

// ── SystemAddress ─────────────────────────────────────────────────────────────

/**
 * Reads a RakNet SystemAddress (1-byte family + address bytes + port).
 * IPv4 address bytes are XOR'd with 0xFF on the wire.
 */
fun ByteBuf.readAddress(): InetSocketAddress {
    return when (val family = readUnsignedByte().toInt()) {
        4 -> {
            val raw = ByteArray(4) { (readByte().toInt() xor 0xFF).toByte() }
            val port = readUnsignedShort()
            InetSocketAddress(Inet4Address.getByAddress(raw), port)
        }
        6 -> {
            skipBytes(2)  // AF_INET6 (platform-dependent, skip)
            val port = readUnsignedShort()
            skipBytes(4)  // flow info
            val raw = ByteArray(16).also { readBytes(it) }
            skipBytes(4)  // scope id
            InetSocketAddress(Inet6Address.getByAddress(null, raw, 0), port)
        }
        else -> throw InvalidPacketException("Unknown address family: $family")
    }
}

fun ByteBuf.writeAddress(address: InetSocketAddress) {
    when (val addr = address.address) {
        is Inet4Address -> {
            writeByte(4)
            for (b in addr.address) writeByte(b.toInt() xor 0xFF)
            writeShort(address.port)
        }
        is Inet6Address -> {
            writeByte(6)
            writeShortLE(23)   // AF_INET6 = 23 on Windows (common RakNet target)
            writeShort(address.port)
            writeInt(0)        // flow info
            writeBytes(addr.address)
            writeInt(0)        // scope id
        }
        else -> throw InvalidPacketException("Unsupported address type: ${addr::class.simpleName}")
    }
}

/** Size in bytes of a serialized IPv4 SystemAddress. */
const val IPV4_ADDRESS_SIZE = 7   // 1 + 4 + 2

/** Size in bytes of a serialized IPv6 SystemAddress. */
const val IPV6_ADDRESS_SIZE = 29  // 1 + 2 + 2 + 4 + 16 + 4

val UNASSIGNED_ADDRESS: InetSocketAddress = InetSocketAddress("0.0.0.0", 0)
