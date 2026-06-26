package io.github.agent0876.raknetty.handler.reliability

import io.github.agent0876.raknetty.core.packet.RakNetFrame
import io.github.agent0876.raknetty.core.packet.SplitInfo
import io.github.agent0876.raknetty.core.protocol.Reliability
import io.netty.buffer.Unpooled
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class FragmentAccumulatorTest {

    private val alloc = Unpooled.EMPTY_BUFFER.alloc()

    private fun frame(splitId: Int, splitCount: Int, index: Int, data: ByteArray): RakNetFrame =
        RakNetFrame(
            reliability = Reliability.RELIABLE_ORDERED,
            split       = SplitInfo(splitId, splitCount, index),
            payload     = Unpooled.wrappedBuffer(data),
        )

    @Test fun `returns null while fragments are still missing`() {
        val acc = FragmentAccumulator()
        assertNull(acc.accumulate(frame(1, 3, 0, byteArrayOf(1)), alloc))
        assertNull(acc.accumulate(frame(1, 3, 1, byteArrayOf(2)), alloc))
    }

    @Test fun `reassembles 3 fragments in order`() {
        val acc = FragmentAccumulator()
        acc.accumulate(frame(1, 3, 0, byteArrayOf(10)), alloc)
        acc.accumulate(frame(1, 3, 1, byteArrayOf(20)), alloc)
        val result = acc.accumulate(frame(1, 3, 2, byteArrayOf(30)), alloc)

        assertNotNull(result)
        assertEquals(3, result.readableBytes())
        assertEquals(10.toByte(), result.readByte())
        assertEquals(20.toByte(), result.readByte())
        assertEquals(30.toByte(), result.readByte())
        result.release()
    }

    @Test fun `reassembles fragments arriving out of order`() {
        val acc = FragmentAccumulator()
        acc.accumulate(frame(2, 3, 2, byteArrayOf(30)), alloc)
        acc.accumulate(frame(2, 3, 0, byteArrayOf(10)), alloc)
        val result = acc.accumulate(frame(2, 3, 1, byteArrayOf(20)), alloc)

        assertNotNull(result)
        result.resetReaderIndex()
        assertEquals(10.toByte(), result.readByte())
        assertEquals(20.toByte(), result.readByte())
        assertEquals(30.toByte(), result.readByte())
        result.release()
    }

    @Test fun `duplicate fragment is silently ignored`() {
        val acc = FragmentAccumulator()
        acc.accumulate(frame(3, 2, 0, byteArrayOf(1)), alloc)
        // duplicate fragment 0
        assertNull(acc.accumulate(frame(3, 2, 0, byteArrayOf(99)), alloc))
        val result = acc.accumulate(frame(3, 2, 1, byteArrayOf(2)), alloc)

        assertNotNull(result)
        assertEquals(1.toByte(), result.readByte())  // original, not the duplicate
        assertEquals(2.toByte(), result.readByte())
        result.release()
    }

    @Test fun `two concurrent split sets are independent`() {
        val acc = FragmentAccumulator()
        acc.accumulate(frame(10, 2, 0, byteArrayOf(1)), alloc)
        acc.accumulate(frame(20, 2, 0, byteArrayOf(9)), alloc)

        val r1 = acc.accumulate(frame(10, 2, 1, byteArrayOf(2)), alloc)
        val r2 = acc.accumulate(frame(20, 2, 1, byteArrayOf(8)), alloc)

        assertNotNull(r1); assertEquals(1.toByte(), r1.readByte()); r1.release()
        assertNotNull(r2); assertEquals(9.toByte(), r2.readByte()); r2.release()
    }
}
