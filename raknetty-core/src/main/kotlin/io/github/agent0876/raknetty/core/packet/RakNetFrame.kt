package io.github.agent0876.raknetty.core.packet

import io.github.agent0876.raknetty.core.protocol.Reliability
import io.netty5.buffer.Buffer

/**
 * A single encapsulated packet frame carried inside a [RakNetDatagram.Data].
 *
 * Index fields ([reliableIndex], [sequenceIndex], [orderIndex]) are assigned by
 * the reliability layer before encoding; callers building outbound frames may
 * leave them at their default of 0.
 *
 * **Lifecycle**: the holder of this frame owns [payload] and must call
 * [payload].close() when done.
 */
data class RakNetFrame(
    val reliability: Reliability,
    val reliableIndex: Int  = 0,  // 24-bit; present if reliability.isReliable
    val sequenceIndex: Int  = 0,  // 24-bit; present if reliability.isSequenced
    val orderIndex: Int     = 0,  // 24-bit; present if isOrdered || isSequenced
    val orderChannel: Int   = 0,  // 0..31;  present if isOrdered || isSequenced
    val split: SplitInfo?   = null,
    val payload: Buffer,
) {
    /**
     * Exact wire size of this frame in bytes.
     * Used by the reliability layer to pack multiple frames into one datagram
     * without exceeding the negotiated MTU.
     *
     * Wire layout:
     *   flags(1) + length(2)
     *   + reliableMessageIndex(3)   if isReliable
     *   + sequencingIndex(3)        if isSequenced
     *   + orderingIndex(3) + orderingChannel(1)  if isOrdered || isSequenced
     *   + splitCount(4) + splitId(2) + splitIndex(4)  if split != null
     *   + payload bytes
     */
    fun wireSize(): Int {
        var n = 3  // flags + length
        if (reliability.isReliable)                         n += 3
        if (reliability.isSequenced)                        n += 3
        if (reliability.isOrdered || reliability.isSequenced) n += 4  // orderIndex(3) + channel(1)
        if (split != null)                                  n += 10 // splitCount(4)+splitId(2)+splitIndex(4)
        n += payload.readableBytes()
        return n
    }
}

/**
 * Fragmentation metadata attached to a frame when a message exceeds the MTU.
 * Frames sharing the same [splitId] are reassembled in [splitIndex] order.
 */
data class SplitInfo(
    val splitId: Int,     // 16-bit; unique per connection per split message
    val splitCount: Int,  // total number of fragments
    val splitIndex: Int,  // 0-based position of this fragment
)
