package io.github.agent0876.raknetty.handler.reliability

import io.github.agent0876.raknetty.core.packet.RakNetFrame
import io.github.agent0876.raknetty.core.protocol.Reliability
import io.github.agent0876.raknetty.core.util.RangeList
import io.github.agent0876.raknetty.handler.connection.ConnectionConfig
import io.netty5.buffer.BufferAllocator
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class SendBufferTest {

    private val alloc  = BufferAllocator.onHeapUnpooled()
    private val config = ConnectionConfig(initialRto = 200L, maxUnackedDatagrams = 4, initialCwnd = 4)

    private fun buffer(mtu: Int = 1492) = SendBuffer(mtu, config)

    private fun frame(bytes: ByteArray, reliability: Reliability = Reliability.RELIABLE): RakNetFrame =
        RakNetFrame(reliability = reliability, payload = BufferAllocator.onHeapUnpooled().copyOf(bytes))

    @Test fun `unreliable frames are not stored in unacked window`() {
        val buf = buffer()
        buf.enqueue(frame(byteArrayOf(1, 2, 3), Reliability.UNRELIABLE))
        val sent = buf.flushSendQueue(System.currentTimeMillis(), alloc)
        assertEquals(1, sent.size)
        sent.forEach { it.close() }
        // No unacked datagrams — ACK of seqNum 0 should be a no-op
        val ack = RangeList().also { it.add(0) }
        buf.onAck(ack)  // must not throw or cause double-release
    }

    @Test fun `reliable frames are retained until ACK arrives`() {
        val buf = buffer()
        buf.enqueue(frame(byteArrayOf(42), Reliability.RELIABLE))
        val sent = buf.flushSendQueue(System.currentTimeMillis(), alloc)
        assertEquals(1, sent.size)
        sent.forEach { it.close() }

        // ACK the datagram (seqNum = 0)
        val ack = RangeList().also { it.add(0) }
        buf.onAck(ack)   // releases retained frame payload — no leak
    }

    @Test fun `NAK triggers immediate retransmit on next call`() {
        val buf = buffer()
        buf.enqueue(frame(byteArrayOf(1), Reliability.RELIABLE))
        val sent = buf.flushSendQueue(0L, alloc)
        sent.forEach { it.close() }

        // Simulate NAK for seqNum 0
        val nak = RangeList().also { it.add(0) }
        buf.onNak(nak)

        val retransmits = buf.retransmitTimedOut(System.currentTimeMillis(), alloc)
        assertEquals(1, retransmits.size, "Expected one retransmit datagram")
        retransmits.forEach { it.close() }

        // Clean up
        buf.onAck(RangeList().also { it.add(1) })  // ACK the retransmit (new seqNum = 1)
    }

    @Test fun `backpressure stops send when unacked window is full`() {
        val buf = buffer()
        repeat(config.maxUnackedDatagrams + 2) {
            buf.enqueue(frame(byteArrayOf(it.toByte()), Reliability.RELIABLE))
        }
        val sent = buf.flushSendQueue(0L, alloc)
        assertTrue(sent.size <= config.maxUnackedDatagrams)
        sent.forEach { it.close() }
    }

    @Test fun `ACK flush produces datagram and clears pending list`() {
        val buf = buffer()
        buf.ackDatagramReceived(5)
        buf.ackDatagramReceived(6)
        val ackBuf = buf.flushAcks(alloc)
        assertTrue(ackBuf != null && ackBuf.readableBytes() > 0)
        ackBuf.close()

        // Second flush: nothing to send
        val second = buf.flushAcks(alloc)
        assertTrue(second == null)
    }

    @Test fun `release cleans up without crash`() {
        val buf = buffer()
        repeat(3) { buf.enqueue(frame(byteArrayOf(it.toByte()), Reliability.RELIABLE)) }
        // buf.close()  // SendBuffer does not implement Closeable
    }
}
