package io.github.agent0876.raknetty.codec

import io.github.agent0876.raknetty.core.packet.RakNetFrame
import io.github.agent0876.raknetty.core.packet.SplitInfo
import io.github.agent0876.raknetty.core.protocol.Reliability
import io.netty.buffer.Unpooled
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class RakNetFrameCodecTest {

    private val alloc = Unpooled.EMPTY_BUFFER.alloc()

    private fun roundtrip(frame: RakNetFrame): RakNetFrame {
        val out = alloc.buffer()
        RakNetFrameCodec.encode(frame, out)
        val decoded = RakNetFrameCodec.decode(out, alloc)
        out.release()
        return decoded
    }

    @Test fun `UNRELIABLE frame roundtrip`() {
        val payload = Unpooled.wrappedBuffer(byteArrayOf(1, 2, 3, 4))
        val frame = RakNetFrame(reliability = Reliability.UNRELIABLE, payload = payload)
        val decoded = roundtrip(frame)

        assertEquals(Reliability.UNRELIABLE, decoded.reliability)
        assertEquals(0, decoded.reliableIndex)
        assertNull(decoded.split)
        assertEquals(4, decoded.payload.readableBytes())
        decoded.payload.release()
        payload.release()
    }

    @Test fun `RELIABLE_ORDERED frame roundtrip preserves indices`() {
        val payload = Unpooled.wrappedBuffer(byteArrayOf(0xDE.toByte(), 0xAD.toByte()))
        val frame = RakNetFrame(
            reliability   = Reliability.RELIABLE_ORDERED,
            reliableIndex = 42,
            orderIndex    = 7,
            orderChannel  = 2,
            payload       = payload,
        )
        val decoded = roundtrip(frame)

        assertEquals(Reliability.RELIABLE_ORDERED, decoded.reliability)
        assertEquals(42, decoded.reliableIndex)
        assertEquals(7,  decoded.orderIndex)
        assertEquals(2,  decoded.orderChannel)
        decoded.payload.release()
        payload.release()
    }

    @Test fun `split frame roundtrip`() {
        val payload = Unpooled.wrappedBuffer(ByteArray(100))
        val frame = RakNetFrame(
            reliability = Reliability.RELIABLE_ORDERED,
            reliableIndex = 1,
            orderIndex = 0,
            split = SplitInfo(splitId = 3, splitCount = 5, splitIndex = 2),
            payload = payload,
        )
        val decoded = roundtrip(frame)

        assertEquals(SplitInfo(3, 5, 2), decoded.split)
        assertEquals(100, decoded.payload.readableBytes())
        decoded.payload.release()
        payload.release()
    }

    @Test fun `wireSize matches actual encoded size`() {
        val payload = Unpooled.wrappedBuffer(ByteArray(64))
        val frame = RakNetFrame(
            reliability = Reliability.RELIABLE_ORDERED,
            reliableIndex = 10,
            orderIndex = 1,
            orderChannel = 0,
            payload = payload,
        )
        val out = alloc.buffer()
        RakNetFrameCodec.encode(frame, out)
        assertEquals(frame.wireSize(), out.readableBytes())
        out.release()
        payload.release()
    }
}
