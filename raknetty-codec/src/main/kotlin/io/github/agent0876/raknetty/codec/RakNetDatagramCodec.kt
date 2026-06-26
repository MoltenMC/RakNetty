package io.github.agent0876.raknetty.codec

import io.github.agent0876.raknetty.core.exception.InvalidPacketException
import io.github.agent0876.raknetty.core.packet.RakNetDatagram
import io.github.agent0876.raknetty.core.protocol.PacketId
import io.github.agent0876.raknetty.core.util.RangeList
import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufAllocator

/**
 * Top-level codec for the outer RakNet datagram (the UDP payload).
 *
 * Inbound first-byte discrimination:
 * ```
 * flags & 0x40 != 0  →  ACK  (0xC0)
 * flags & 0x20 != 0  →  NAK  (0xA0)
 * flags & 0x80 != 0  →  Data (0x84, 0x8C, …)
 * else               →  offline packet — handled by OfflinePacketCodec
 * ```
 *
 * **This codec does not consume offline packets.** The caller must check
 * [isOnlineDatagram] first; offline bytes should be passed to [OfflinePacketCodec].
 */
object RakNetDatagramCodec {

    /** Returns true when [firstByte] belongs to an online (data/ack/nak) datagram. */
    fun isOnlineDatagram(firstByte: Int): Boolean = (firstByte and PacketId.FLAG_VALID) != 0

    // ── Decode ────────────────────────────────────────────────────────────────

    /**
     * Decodes a full datagram payload starting at [buf.readerIndex].
     * Allocates [RakNetFrame.payload] buffers from [alloc].
     */
    fun decode(buf: ByteBuf, alloc: ByteBufAllocator): RakNetDatagram {
        val flags = buf.readUnsignedByte().toInt()
        return when {
            flags and PacketId.FLAG_ACK != 0 -> RakNetDatagram.Ack(RangeList.decode(buf))
            flags and PacketId.FLAG_NAK != 0 -> RakNetDatagram.Nak(RangeList.decode(buf))
            flags and PacketId.FLAG_VALID != 0 -> decodeData(flags, buf, alloc)
            else -> throw InvalidPacketException(
                "Expected online datagram (0x80+), got 0x${flags.toString(16).padStart(2, '0')}"
            )
        }
    }

    private fun decodeData(flags: Int, buf: ByteBuf, alloc: ByteBufAllocator): RakNetDatagram.Data {
        val seqNum = buf.readUnsignedMediumLE()
        val frames = buildList {
            while (buf.isReadable) add(RakNetFrameCodec.decode(buf, alloc))
        }
        return RakNetDatagram.Data(flags = flags, sequenceNumber = seqNum, frames = frames)
    }

    // ── Encode ────────────────────────────────────────────────────────────────

    fun encode(datagram: RakNetDatagram, out: ByteBuf) {
        when (datagram) {
            is RakNetDatagram.Ack  -> encodeAckOrNak(PacketId.FLAG_VALID or PacketId.FLAG_ACK, datagram.ranges, out)
            is RakNetDatagram.Nak  -> encodeAckOrNak(PacketId.FLAG_VALID or PacketId.FLAG_NAK, datagram.ranges, out)
            is RakNetDatagram.Data -> encodeData(datagram, out)
        }
    }

    private fun encodeAckOrNak(flags: Int, ranges: RangeList, out: ByteBuf) {
        out.writeByte(flags)
        ranges.encode(out)
    }

    private fun encodeData(datagram: RakNetDatagram.Data, out: ByteBuf) {
        out.writeByte(datagram.flags)
        out.writeMediumLE(datagram.sequenceNumber)
        for (frame in datagram.frames) RakNetFrameCodec.encode(frame, out)
    }
}
