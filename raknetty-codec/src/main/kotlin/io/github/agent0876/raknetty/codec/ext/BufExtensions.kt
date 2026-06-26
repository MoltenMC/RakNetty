package io.github.agent0876.raknetty.codec.ext

import io.github.agent0876.raknetty.core.exception.InvalidPacketException
import io.github.agent0876.raknetty.core.protocol.PacketId
import io.netty5.buffer.Buffer
import java.net.Inet4Address
import java.net.Inet6Address
import java.net.InetSocketAddress
import java.nio.ByteBuffer

fun Buffer.writeMagic() {
    writeBytes(ByteBuffer.wrap(PacketId.OFFLINE_MESSAGE_ID))
}

fun Buffer.readMagic(): Boolean {
    val bytes = ByteArray(16)
    readBytes(ByteBuffer.wrap(bytes))
    return bytes.contentEquals(PacketId.OFFLINE_MESSAGE_ID)
}

fun Buffer.verifyMagic() {
    if (!readMagic()) throw InvalidPacketException("Offline message magic mismatch")
}

fun Buffer.readAddress(): InetSocketAddress {
    return when (val family = readUnsignedByte().toInt()) {
        4 -> {
            val raw = ByteArray(4) { (readByte().toInt() xor 0xFF).toByte() }
            val port = readUnsignedShort()
            InetSocketAddress(Inet4Address.getByAddress(raw), port)
        }
        6 -> {
            skipReadableBytes(2)
            val port = readUnsignedShort()
            skipReadableBytes(4)
            val raw = ByteArray(16)
            readBytes(ByteBuffer.wrap(raw))
            skipReadableBytes(4)
            InetSocketAddress(Inet6Address.getByAddress(null, raw, 0), port)
        }
        else -> throw InvalidPacketException("Unknown address family: $family")
    }
}

fun Buffer.writeAddress(address: InetSocketAddress) {
    when (val addr = address.address) {
        is Inet4Address -> {
            writeByte(4.toByte())
            for (b in addr.address) writeByte((b.toInt() xor 0xFF).toByte())
            writeShort(address.port.toShort())
        }
        is Inet6Address -> {
            writeByte(6.toByte())
            // write AF_INET6 = 23 in little-endian (2 bytes)
            writeByte(23.toByte())
            writeByte(0.toByte())
            writeShort(address.port.toShort())
            writeInt(0)
            writeBytes(ByteBuffer.wrap(addr.address))
            writeInt(0)
        }
        else -> throw InvalidPacketException("Unsupported address type: ${addr::class.simpleName}")
    }
}

const val IPV4_ADDRESS_SIZE = 7

const val IPV6_ADDRESS_SIZE = 29

val UNASSIGNED_ADDRESS: InetSocketAddress = InetSocketAddress("0.0.0.0", 0)
