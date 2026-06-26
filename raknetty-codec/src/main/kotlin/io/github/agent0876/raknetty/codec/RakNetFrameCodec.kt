package io.github.agent0876.raknetty.codec

import io.github.agent0876.raknetty.core.packet.RakNetFrame
import io.github.agent0876.raknetty.core.packet.SplitInfo
import io.github.agent0876.raknetty.core.protocol.Reliability
import io.github.agent0876.raknetty.core.util.readUnsignedMediumLE
import io.github.agent0876.raknetty.core.util.writeMediumLE
import io.netty5.buffer.Buffer
import io.netty5.buffer.BufferAllocator
import java.nio.ByteBuffer

/**
 * Encodes and decodes [RakNetFrame]s within a data datagram payload.
 *
 * Frame wire layout:
 * ```
 * flags         (1B)  — bits[7:5] = reliability id, bit[4] = hasSplit
 * length        (2B)  — payload length in bits, big-endian
 * reliableIndex (3B)  — LE, if isReliable
 * sequenceIndex (3B)  — LE, if isSequenced
 * orderIndex    (3B)  — LE, if isOrdered || isSequenced
 * orderChannel  (1B)  — if isOrdered || isSequenced
 * splitCount    (4B)  — if hasSplit
 * splitId       (2B)  — if hasSplit
 * splitIndex    (4B)  — if hasSplit
 * payload       (N B)
 * ```
 */
object RakNetFrameCodec {

    fun decode(buf: Buffer, alloc: BufferAllocator): RakNetFrame {
        val flags       = buf.readUnsignedByte().toInt()
        val reliability = Reliability.fromId(flags ushr 5)
        val hasSplit    = (flags and 0x10) != 0

        val lengthBits  = buf.readUnsignedShort()
        val payloadLen  = (lengthBits + 7) / 8

        val reliableIndex = if (reliability.isReliable)   buf.readUnsignedMediumLE() else 0
        val sequenceIndex = if (reliability.isSequenced)  buf.readUnsignedMediumLE() else 0

        val hasOrder = reliability.isOrdered || reliability.isSequenced
        val orderIndex   = if (hasOrder) buf.readUnsignedMediumLE() else 0
        val orderChannel = if (hasOrder) buf.readUnsignedByte().toInt() else 0

        val split = if (hasSplit) {
            val count = buf.readInt()
            val id    = buf.readUnsignedShort()
            val index = buf.readInt()
            SplitInfo(splitId = id, splitCount = count, splitIndex = index)
        } else null

        val payload = buf.copy(buf.readerOffset(), payloadLen)
        buf.skipReadableBytes(payloadLen)

        return RakNetFrame(
            reliability    = reliability,
            reliableIndex  = reliableIndex,
            sequenceIndex  = sequenceIndex,
            orderIndex     = orderIndex,
            orderChannel   = orderChannel,
            split          = split,
            payload        = payload,
        )
    }

    fun encode(frame: RakNetFrame, out: Buffer) {
        val flags = (frame.reliability.id shl 5) or (if (frame.split != null) 0x10 else 0)
        out.writeByte(flags.toByte())
        out.writeShort((frame.payload.readableBytes() * 8).toShort())   // length in bits

        if (frame.reliability.isReliable)  out.writeMediumLE(frame.reliableIndex)
        if (frame.reliability.isSequenced) out.writeMediumLE(frame.sequenceIndex)
        if (frame.reliability.isOrdered || frame.reliability.isSequenced) {
            out.writeMediumLE(frame.orderIndex)
            out.writeByte(frame.orderChannel.toByte())
        }
        frame.split?.let {
            out.writeInt(it.splitCount)
            out.writeShort(it.splitId.toShort())
            out.writeInt(it.splitIndex)
        }
        val len = frame.payload.readableBytes()
        val temp = ByteArray(len)
        frame.payload.copyInto(frame.payload.readerOffset(), temp, 0, len)
        out.writeBytes(ByteBuffer.wrap(temp, 0, len))
    }
}
