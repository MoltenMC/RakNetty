package io.github.agent0876.raknetty.handler.reliability

import io.netty5.buffer.BufferAllocator
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class ReceiveBufferTest {

    private val alloc = BufferAllocator.onHeapUnpooled()

    // ── Datagram gap detection ────────────────────────────────────────────────

    @Test fun `in-order datagram produces no NAK`() {
        val buf = ReceiveBuffer()
        assertNull(buf.onDatagramArrived(0))
        assertNull(buf.onDatagramArrived(1))
        assertNull(buf.onDatagramArrived(2))
    }

    @Test fun `gap in sequence generates NAK range`() {
        val buf = ReceiveBuffer()
        assertNull(buf.onDatagramArrived(0))
        val nak = buf.onDatagramArrived(5)
        assertNotNull(nak)
        assertEquals(1, nak.first)
        assertEquals(4, nak.second)
    }

    @Test fun `out-of-order old datagram produces no NAK`() {
        val buf = ReceiveBuffer()
        buf.onDatagramArrived(5) // jump ahead
        assertNull(buf.onDatagramArrived(3)) // old — should be accepted silently
    }

    // ── Reliable deduplication ─────────────────────────────────────────────────

    @Test fun `isNewReliable returns true for unseen index`() {
        val buf = ReceiveBuffer()
        assertTrue(buf.isNewReliable(0))
        assertTrue(buf.isNewReliable(100))
    }

    @Test fun `isNewReliable returns false after marking received`() {
        val buf = ReceiveBuffer()
        assertTrue(buf.isNewReliable(42))
        buf.markReliableReceived(42)
        assertFalse(buf.isNewReliable(42))
    }

    @Test fun `markReliableReceived does not affect other indices`() {
        val buf = ReceiveBuffer()
        buf.markReliableReceived(5)
        assertTrue(buf.isNewReliable(6))
    }

    // ── Ordered delivery ──────────────────────────────────────────────────────

    @Test fun `drainOrdered returns nothing when gap exists`() {
        val buf = ReceiveBuffer()
        buf.enqueueOrdered(0, 1, alloc.allocate(1))
        assertTrue(buf.drainOrdered(0).isEmpty())
    }

    @Test fun `drainOrdered returns contiguous packets`() {
        val buf = ReceiveBuffer()
        buf.enqueueOrdered(0, 0, alloc.copyOf(byteArrayOf(10)))
        buf.enqueueOrdered(0, 1, alloc.copyOf(byteArrayOf(20)))
        buf.enqueueOrdered(0, 2, alloc.copyOf(byteArrayOf(30)))

        val drained = buf.drainOrdered(0)
        assertEquals(3, drained.size)
        assertEquals(10, drained[0].readByte().toInt() and 0xFF)
        assertEquals(20, drained[1].readByte().toInt() and 0xFF)
        assertEquals(30, drained[2].readByte().toInt() and 0xFF)
        drained.forEach { it.close() }
    }

    @Test fun `drainOrdered skips ahead after previous drain`() {
        val buf = ReceiveBuffer()
        buf.enqueueOrdered(0, 0, alloc.copyOf(byteArrayOf(1)))
        assertEquals(1, buf.drainOrdered(0).size)

        buf.enqueueOrdered(0, 1, alloc.copyOf(byteArrayOf(2)))
        buf.enqueueOrdered(0, 2, alloc.copyOf(byteArrayOf(3)))
        val drained = buf.drainOrdered(0)
        assertEquals(2, drained.size)
        drained.forEach { it.close() }
    }

    // ── Sequenced delivery ────────────────────────────────────────────────────

    @Test fun `isNewerSequenced returns true initially`() {
        val buf = ReceiveBuffer()
        assertTrue(buf.isNewerSequenced(0, 0))
        assertTrue(buf.isNewerSequenced(1, 100))
    }

    @Test fun `isNewerSequenced returns false for stale indices`() {
        val buf = ReceiveBuffer()
        buf.updateSequenced(0, 10)
        assertFalse(buf.isNewerSequenced(0, 5))
    }

    @Test fun `isNewerSequenced returns true for newer indices`() {
        val buf = ReceiveBuffer()
        buf.updateSequenced(0, 10)
        assertTrue(buf.isNewerSequenced(0, 11))
    }

    // ── Release ───────────────────────────────────────────────────────────────

    @Test fun `release does not throw`() {
        val buf = ReceiveBuffer()
        buf.enqueueOrdered(0, 0, alloc.copyOf(byteArrayOf(1)))
        buf.enqueueOrdered(0, 1, alloc.copyOf(byteArrayOf(2)))
        buf.release()
    }

}
