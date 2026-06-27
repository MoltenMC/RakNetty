package io.github.moltenmc.raknetty.core.util

import io.netty5.buffer.Buffer
import java.nio.ByteBuffer

fun Buffer.readBytes(dst: ByteArray) = readBytes(ByteBuffer.wrap(dst))
fun Buffer.writeBytes(src: ByteArray) = writeBytes(ByteBuffer.wrap(src))

fun Buffer.writeByte(value: Int) = writeByte(value.toByte())

fun Buffer.writeShort(value: Int) = writeShort(value.toShort())

fun Buffer.readUnsignedMediumLE(): Int {
    val b1 = readUnsignedByte()
    val b2 = readUnsignedByte()
    val b3 = readUnsignedByte()
    return b1 or (b2 shl 8) or (b3 shl 16)
}

fun Buffer.writeMediumLE(value: Int) {
    writeByte(value and 0xFF)
    writeByte((value ushr 8) and 0xFF)
    writeByte((value ushr 16) and 0xFF)
}

fun Buffer.writeShortLE(value: Int) {
    writeByte(value and 0xFF)
    writeByte((value ushr 8) and 0xFF)
}
