package io.github.moltenmc.raknetty.handler.reliability

import io.github.moltenmc.raknetty.core.protocol.RakNetProtocol
import io.github.moltenmc.raknetty.core.util.SequenceNumber
import io.netty5.buffer.Buffer
import java.util.TreeMap

/**
 * Inbound reliability state for one connection.
 *
 * Responsibilities:
 * - Datagram gap detection → produces NAK ranges
 * - Reliable-frame deduplication via reliableMessageIndex
 * - Per-channel ordered delivery (hold-and-release reorder buffer)
 * - Per-channel sequenced delivery (drop older than highest-seen)
 */
class ReceiveBuffer {

    // ── Datagram sequence tracking ────────────────────────────────────────────

    private var nextExpectedDatagramSeq = 0

    /**
     * Records arrival of a datagram with [seqNum].
     * Returns a list of (start, end) NAK ranges if a gap was detected, or an empty list.
     * Two ranges are returned when the gap spans the 24-bit sequence-number wrap boundary
     * (e.g. gap [16_777_213..0] is split into [16_777_213..MAX] and [0..0]).
     *
     * Note: this simplified implementation does not handle out-of-order arrivals
     * that arrive before the gap is filled (they are treated as "old"). The reliable
     * message deduplication layer handles the actual content correctness.
     */
    fun onDatagramArrived(seqNum: Int): List<Pair<Int, Int>> {
        return when {
            seqNum == nextExpectedDatagramSeq -> {
                nextExpectedDatagramSeq = SequenceNumber.increment(seqNum)
                emptyList()
            }
            SequenceNumber.isGreaterThan(seqNum, nextExpectedDatagramSeq) -> {
                val nakStart = nextExpectedDatagramSeq
                val nakEnd   = SequenceNumber.increment(seqNum, -1)
                nextExpectedDatagramSeq = SequenceNumber.increment(seqNum)
                if (nakEnd < nakStart) {
                    // Gap spans the 24-bit wrap boundary: split into two contiguous ranges.
                    listOf(Pair(nakStart, SequenceNumber.MAX), Pair(0, nakEnd))
                } else {
                    listOf(Pair(nakStart, nakEnd))
                }
            }
            else -> emptyList()  // old or duplicate datagram — still process its frames for dedup
        }
    }

    // ── Reliable message deduplication ────────────────────────────────────────

    // LinkedHashSet gives O(1) contains and insertion-order removal for sliding window.
    private val receivedReliable = LinkedHashSet<Int>()

    fun isNewReliable(reliableIdx: Int): Boolean = reliableIdx !in receivedReliable

    fun markReliableReceived(reliableIdx: Int) {
        receivedReliable += reliableIdx
        if (receivedReliable.size > RELIABLE_DEDUP_WINDOW) {
            receivedReliable.remove(receivedReliable.first())
        }
    }

    // ── Ordered delivery ──────────────────────────────────────────────────────

    // Per-channel expected orderIndex and hold buffer for out-of-order arrivals
    private val orderExpected = IntArray(RakNetProtocol.MAX_ORDER_CHANNELS)
    private val orderBuffers  = Array(RakNetProtocol.MAX_ORDER_CHANNELS) { TreeMap<Int, Buffer>() }

    /** Enqueues [payload] (retained by caller) for ordered delivery on [channel]. */
    fun enqueueOrdered(channel: Int, orderIndex: Int, payload: Buffer) {
        orderBuffers[channel][orderIndex] = payload
    }

    /**
     * Drains all contiguous in-order payloads from [channel].
     * Each returned [Buffer] is owned by the caller and must be released.
     */
    fun drainOrdered(channel: Int): List<Buffer> {
        val result = mutableListOf<Buffer>()
        val buf = orderBuffers[channel]
        while (true) {
            val next = buf.remove(orderExpected[channel]) ?: break
            result += next
            orderExpected[channel] = SequenceNumber.increment(orderExpected[channel])
        }
        return result
    }

    // ── Sequenced delivery ────────────────────────────────────────────────────

    private val sequenceHighest = IntArray(RakNetProtocol.MAX_ORDER_CHANNELS) { -1 }

    fun isNewerSequenced(channel: Int, seqIndex: Int): Boolean {
        val highest = sequenceHighest[channel]
        return highest == -1 || SequenceNumber.isGreaterThan(seqIndex, highest)
    }

    fun updateSequenced(channel: Int, seqIndex: Int) {
        sequenceHighest[channel] = seqIndex
    }

    /** Releases all pending ordered buffers. Call when the connection closes. */
    fun release() {
        for (buf in orderBuffers) buf.values.forEach { it.close() }
        for (buf in orderBuffers) buf.clear()
    }

    companion object {
        private const val RELIABLE_DEDUP_WINDOW = 1024
    }
}
