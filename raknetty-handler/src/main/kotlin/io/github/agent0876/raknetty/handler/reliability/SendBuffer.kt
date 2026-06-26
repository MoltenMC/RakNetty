package io.github.agent0876.raknetty.handler.reliability

import io.github.agent0876.raknetty.codec.RakNetDatagramCodec
import io.github.agent0876.raknetty.core.packet.RakNetDatagram
import io.github.agent0876.raknetty.core.packet.RakNetFrame
import io.github.agent0876.raknetty.core.protocol.PacketId
import io.github.agent0876.raknetty.core.protocol.RakNetProtocol
import io.github.agent0876.raknetty.core.util.RangeList
import io.github.agent0876.raknetty.core.util.SequenceNumber
import io.github.agent0876.raknetty.handler.connection.ConnectionConfig
import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufAllocator
import java.util.TreeMap
import kotlin.math.abs

/**
 * Outbound reliability state for one connection.
 *
 * Responsibilities:
 * - Sequence-number assignment (datagram, reliable, order, sequence)
 * - Frame queuing and MTU-aware datagram packing
 * - Unacknowledged datagram tracking and retransmission
 * - Pending ACK/NAK aggregation for the next tick
 * - Dynamic RTO via Jacobson/Karels (RFC 6298) and AIMD congestion control
 *
 * **All methods must be called from the connection's EventLoop thread.**
 */
class SendBuffer(private val mtu: Int, private val config: ConnectionConfig) {

    // ── Sequence counters ─────────────────────────────────────────────────────

    private var nextDatagramSeq  = 0
    private var nextReliableIdx  = 0
    private val orderIndices     = IntArray(RakNetProtocol.MAX_ORDER_CHANNELS)
    private val sequenceIndices  = IntArray(RakNetProtocol.MAX_ORDER_CHANNELS)

    fun nextReliableIndex(): Int   = nextReliableIdx.also  { nextReliableIdx  = SequenceNumber.increment(it) }
    fun nextOrderIndex(ch: Int)    = orderIndices[ch].also    { orderIndices[ch]    = SequenceNumber.increment(it) }
    fun nextSequenceIndex(ch: Int) = sequenceIndices[ch].also { sequenceIndices[ch] = SequenceNumber.increment(it) }

    // ── Outbound queue ────────────────────────────────────────────────────────

    private val sendQueue = ArrayDeque<RakNetFrame>()

    /** Enqueues [frame] for sending. Caller transfers ownership of [frame.payload]. */
    fun enqueue(frame: RakNetFrame) { sendQueue.addLast(frame) }

    // ── Unacknowledged datagram window ────────────────────────────────────────

    private class PendingDatagram(
        val seqNum: Int,
        val reliableFrames: List<RakNetFrame>,  // only reliable frames; payload owned
        var sentAt: Long,
    )

    private val unacked             = TreeMap<Int, PendingDatagram>()
    private val immediateRetransmit = mutableSetOf<Int>()   // NAK-triggered

    // ── ACK/NAK accumulation (flushed each tick) ──────────────────────────────

    private val pendingAcks = RangeList()
    private val pendingNaks = RangeList()

    fun ackDatagramReceived(seqNum: Int) { pendingAcks.add(seqNum) }
    fun nakRange(start: Int, end: Int)   { pendingNaks.addRange(start, end) }

    // ── Dynamic RTO — Jacobson/Karels (RFC 6298) ──────────────────────────────

    private var srtt: Double = -1.0          // smoothed RTT (ms); -1 = no sample yet
    private var rttvar: Double = 0.0         // RTT mean deviation (ms)
    private var rto: Long = config.initialRto

    /** Current retransmission timeout, updated after each ACK with an RTT sample. */
    val currentRto: Long get() = rto

    private fun updateRtt(sampleMs: Long) {
        val r = sampleMs.toDouble()
        if (srtt < 0.0) {
            // First sample: RFC 6298 § 2.2
            srtt   = r
            rttvar = r / 2.0
        } else {
            val err = r - srtt
            srtt   += err / 8.0                          // alpha = 1/8
            rttvar += (abs(err) - rttvar) / 4.0          // beta  = 1/4
        }
        rto = (srtt + 4.0 * rttvar).toLong()
            .coerceIn(config.minRto, config.maxRto)
    }

    // ── AIMD congestion control ───────────────────────────────────────────────

    private var cwnd: Double     = config.initialCwnd.toDouble()
    private var ssthresh: Double = config.maxUnackedDatagrams.toDouble()
    // Prevents multiple congestion reactions within a single congestion epoch.
    private var congestionReacted: Boolean = false

    private fun onCongestionNak() {
        if (congestionReacted) return
        congestionReacted = true
        ssthresh = (cwnd / 2.0).coerceAtLeast(1.0)
        cwnd     = ssthresh   // fast recovery: skip all the way to CA threshold
    }

    private fun onCongestionTimeout() {
        if (congestionReacted) return
        congestionReacted = true
        ssthresh = (cwnd / 2.0).coerceAtLeast(1.0)
        cwnd     = 1.0        // full slow-start after timeout
    }

    // ── Called when remote sends us ACK / NAK ─────────────────────────────────

    /**
     * Process inbound ACK ranges. [now] is used to compute RTT samples.
     * Defaults to the current wall-clock time so callers that already have a
     * timestamp can avoid an extra syscall.
     */
    fun onAck(ranges: RangeList, now: Long = System.currentTimeMillis()) {
        for (r in ranges.ranges) for (seq in r) {
            val pending = unacked.remove(seq) ?: continue
            // RTT sample
            updateRtt(now - pending.sentAt)
            // AIMD growth
            if (cwnd < ssthresh) {
                cwnd += 1.0               // slow start: +1 datagram per ACK
            } else {
                cwnd += 1.0 / cwnd        // congestion avoidance
            }
            cwnd = cwnd.coerceAtMost(config.maxUnackedDatagrams.toDouble())
            pending.reliableFrames.forEach { it.payload.release() }
        }
        // ACKs advancing the window mean we're in a new congestion epoch.
        congestionReacted = false
    }

    fun onNak(ranges: RangeList) {
        for (r in ranges.ranges) for (seq in r) {
            if (unacked.containsKey(seq)) immediateRetransmit += seq
        }
        onCongestionNak()
    }

    // ── IMMEDIATE send (bypass queue) ────────────────────────────────────────

    /**
     * Encodes [frame] into a datagram and returns it for immediate dispatch.
     * Reliable frames are added to the unacked window for retransmission;
     * unreliable frames have their payload released right away.
     *
     * Callers are responsible for writing and flushing the returned [ByteBuf].
     */
    fun sendImmediate(frame: RakNetFrame, now: Long, alloc: ByteBufAllocator): ByteBuf {
        val seqNum  = advanceDatagramSeq()
        val encoded = encodeDatagram(seqNum, listOf(frame), alloc)
        if (frame.reliability.isReliable) {
            unacked[seqNum] = PendingDatagram(seqNum, listOf(frame), now)
        } else {
            frame.payload.release()
        }
        return encoded
    }

    // ── Tick output (call once per tick) ─────────────────────────────────────

    fun flushAcks(alloc: ByteBufAllocator): ByteBuf? {
        if (pendingAcks.isEmpty()) return null
        return alloc.buffer().also { RakNetDatagramCodec.encode(RakNetDatagram.Ack(pendingAcks), it) }
            .also { pendingAcks.clear() }
    }

    fun flushNaks(alloc: ByteBufAllocator): ByteBuf? {
        if (pendingNaks.isEmpty()) return null
        return alloc.buffer().also { RakNetDatagramCodec.encode(RakNetDatagram.Nak(pendingNaks), it) }
            .also { pendingNaks.clear() }
    }

    /** Returns encoded datagrams for frames that timed out or were NAK'd. */
    fun retransmitTimedOut(now: Long, alloc: ByteBufAllocator): List<ByteBuf> {
        val result = mutableListOf<ByteBuf>()

        fun retransmit(pending: PendingDatagram) {
            val newSeq = advanceDatagramSeq()
            result += encodeDatagram(newSeq, pending.reliableFrames, alloc)
            unacked.remove(pending.seqNum)
            unacked[newSeq] = PendingDatagram(newSeq, pending.reliableFrames, now)
        }

        // NAK-triggered retransmits (congestion already signalled in onNak)
        for (seqNum in immediateRetransmit.toList()) unacked[seqNum]?.let(::retransmit)
        immediateRetransmit.clear()

        // Timeout-triggered: apply full slow-start backoff once per epoch
        var timedOut = false
        for (pending in unacked.values.toList()) {
            if (now - pending.sentAt >= rto) {
                retransmit(pending)
                timedOut = true
            }
        }
        if (timedOut) onCongestionTimeout()

        return result
    }

    /**
     * Packs queued frames into datagrams and returns encoded [ByteBuf]s to send.
     * Send rate is governed by [cwnd]; [config.maxUnackedDatagrams] is the hard cap.
     */
    fun flushSendQueue(now: Long, alloc: ByteBufAllocator): List<ByteBuf> {
        val window = cwnd.toInt().coerceAtLeast(1)
        if (sendQueue.isEmpty() || unacked.size >= window) return emptyList()

        val maxPayload = mtu - 28   // subtract IP(20) + UDP(8)
        val result = mutableListOf<ByteBuf>()

        while (sendQueue.isNotEmpty() && unacked.size < window) {
            val frames = mutableListOf<RakNetFrame>()
            var wireSize = 4  // datagram header: flags(1) + seqNum(3)

            while (sendQueue.isNotEmpty()) {
                val next = sendQueue.first()
                val nextWire = next.wireSize()
                if (wireSize + nextWire > maxPayload && frames.isNotEmpty()) break
                frames += sendQueue.removeFirst()
                wireSize += nextWire
            }

            val seqNum = advanceDatagramSeq()
            result += encodeDatagram(seqNum, frames, alloc)

            val reliableFrames = frames.filter { it.reliability.isReliable }
            frames.filter { !it.reliability.isReliable }.forEach { it.payload.release() }
            if (reliableFrames.isNotEmpty()) {
                unacked[seqNum] = PendingDatagram(seqNum, reliableFrames, now)
            }
        }

        return result
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private fun advanceDatagramSeq(): Int =
        nextDatagramSeq.also { nextDatagramSeq = SequenceNumber.increment(it) }

    private fun encodeDatagram(seqNum: Int, frames: List<RakNetFrame>, alloc: ByteBufAllocator): ByteBuf {
        val datagram = RakNetDatagram.Data(PacketId.FLAG_VALID, seqNum, frames)
        return alloc.buffer().also { RakNetDatagramCodec.encode(datagram, it) }
    }

    /** Releases all retained payloads. Call when the connection closes. */
    fun release() {
        sendQueue.forEach { it.payload.release() }
        sendQueue.clear()
        unacked.values.forEach { p -> p.reliableFrames.forEach { it.payload.release() } }
        unacked.clear()
    }
}
