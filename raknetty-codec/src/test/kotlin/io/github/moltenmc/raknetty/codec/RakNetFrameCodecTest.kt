package io.github.moltenmc.raknetty.codec

import io.github.moltenmc.raknetty.core.packet.RakNetFrame
import io.github.moltenmc.raknetty.core.packet.SplitInfo
import io.github.moltenmc.raknetty.core.protocol.Reliability
import io.netty5.buffer.BufferAllocator
import java.nio.ByteBuffer
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class RakNetFrameCodecTest {

    private val alloc = BufferAllocator.onHeapUnpooled()

    private fun roundtrip(frame: RakNetFrame): RakNetFrame {
        val out = alloc.allocate(256)
        RakNetFrameCodec.encode(frame, out)
        val decoded = RakNetFrameCodec.decode(out, alloc)
        out.close()
        return decoded
    }

    @Test fun `UNRELIABLE frame roundtrip`() {
        val payload = alloc.copyOf(byteArrayOf(1, 2, 3, 4))
        val frame = RakNetFrame(reliability = Reliability.UNRELIABLE, payload = payload)
        val decoded = roundtrip(frame)

        assertEquals(Reliability.UNRELIABLE, decoded.reliability)
        assertEquals(0, decoded.reliableIndex)
        assertNull(decoded.split)
        assertEquals(4, decoded.payload.readableBytes())
        decoded.payload.close()
        payload.close()
    }

    @Test fun `RELIABLE_ORDERED frame roundtrip preserves indices`() {
        val payload = alloc.copyOf(byteArrayOf(0xDE.toByte(), 0xAD.toByte()))
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
        decoded.payload.close()
        payload.close()
    }

    @Test fun `split frame roundtrip`() {
        val payload = alloc.allocate(100).also { it.writeBytes(ByteBuffer.wrap(ByteArray(100))) }
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
        decoded.payload.close()
        payload.close()
    }

    @Test fun `wireSize matches actual encoded size`() {
        val payload = alloc.allocate(64).also { it.writeBytes(ByteBuffer.wrap(ByteArray(64))) }
        val frame = RakNetFrame(
            reliability = Reliability.RELIABLE_ORDERED,
            reliableIndex = 10,
            orderIndex = 1,
            orderChannel = 0,
            payload = payload,
        )
        val out = alloc.allocate(256)
        RakNetFrameCodec.encode(frame, out)
        assertEquals(frame.wireSize(), out.readableBytes())
        out.close()
        payload.close()
    }
}
