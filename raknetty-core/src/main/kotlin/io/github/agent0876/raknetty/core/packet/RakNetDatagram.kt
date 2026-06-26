package io.github.agent0876.raknetty.core.packet

import io.github.agent0876.raknetty.core.protocol.PacketId
import io.github.agent0876.raknetty.core.util.RangeList

/**
 * Top-level RakNet datagram, discriminated by the first byte of the UDP payload.
 *
 * ```
 * 0x80..0xBF  → Data  (FLAG_VALID set, FLAG_ACK clear, FLAG_NAK clear)
 * 0xC0..0xDF  → Ack   (FLAG_VALID | FLAG_ACK)
 * 0xA0..0xBF  → Nak   (FLAG_VALID | FLAG_NAK)
 * ```
 *
 * Decoding and encoding is handled by `RakNetDatagramCodec` in `raknetty-codec`.
 */
sealed class RakNetDatagram {

    /**
     * A datagram carrying one or more [RakNetFrame]s.
     * [sequenceNumber] is a 24-bit rolling counter used for ACK/NAK tracking.
     *
     * Call [release] when the datagram is fully consumed to free [RakNetFrame.payload] references.
     */
    data class Data(
        val flags: Int,
        val sequenceNumber: Int,
        val frames: List<RakNetFrame>,
    ) : RakNetDatagram() {
        val isPacketPair: Boolean     get() = flags and PacketId.FLAG_PACKET_PAIR     != 0
        val isContinuousSend: Boolean get() = flags and PacketId.FLAG_CONTINUOUS_SEND != 0
        val needsBAndAs: Boolean      get() = flags and PacketId.FLAG_NEEDS_B_AND_AS  != 0

        fun release() = frames.forEach { it.payload.close() }
    }

    /** Acknowledges receipt of the datagram sequence numbers in [ranges]. */
    data class Ack(val ranges: RangeList) : RakNetDatagram()

    /** Requests retransmission of the datagram sequence numbers in [ranges]. */
    data class Nak(val ranges: RangeList) : RakNetDatagram()
}
