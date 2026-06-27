package io.github.moltenmc.raknetty.codec

import io.github.moltenmc.raknetty.core.packet.RakNetDatagram
import io.github.moltenmc.raknetty.core.packet.RakNetFrame
import io.github.moltenmc.raknetty.core.protocol.PacketId
import io.github.moltenmc.raknetty.core.protocol.Reliability
import io.github.moltenmc.raknetty.core.util.RangeList
import io.netty5.buffer.BufferAllocator
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class RakNetDatagramCodecTest {

    private val alloc = BufferAllocator.onHeapUnpooled()

    @Test fun `isOnlineDatagram returns true for 0x80 and above`() {
        assertTrue(RakNetDatagramCodec.isOnlineDatagram(0x80))
        assertTrue(RakNetDatagramCodec.isOnlineDatagram(0x84))
        assertTrue(RakNetDatagramCodec.isOnlineDatagram(0xC0))
        assertTrue(RakNetDatagramCodec.isOnlineDatagram(0xA0))
    }

    @Test fun `isOnlineDatagram returns false for offline IDs`() {
        assertFalse(RakNetDatagramCodec.isOnlineDatagram(0x00))
        assertFalse(RakNetDatagramCodec.isOnlineDatagram(0x01))
        assertFalse(RakNetDatagramCodec.isOnlineDatagram(0x1C))
        assertFalse(RakNetDatagramCodec.isOnlineDatagram(0x7F))
    }

    @Test fun `ACK roundtrip`() {
        val ranges = RangeList().also {
            it.add(0)
            it.add(5)
            it.addRange(10, 15)
        }
        val original = RakNetDatagram.Ack(ranges)

        val buf = alloc.allocate(64)
        RakNetDatagramCodec.encode(original, buf)
        val decoded = RakNetDatagramCodec.decode(buf, alloc)
        buf.close()

        assertTrue(decoded is RakNetDatagram.Ack)
        assertEquals(ranges.ranges, decoded.ranges.ranges)
    }

    @Test fun `NAK roundtrip`() {
        val ranges = RangeList().also { it.addRange(3, 7) }
        val original = RakNetDatagram.Nak(ranges)

        val buf = alloc.allocate(64)
        RakNetDatagramCodec.encode(original, buf)
        val decoded = RakNetDatagramCodec.decode(buf, alloc)
        buf.close()

        assertTrue(decoded is RakNetDatagram.Nak)
        assertEquals(ranges.ranges, decoded.ranges.ranges)
    }

    @Test fun `Data datagram with one frame roundtrip`() {
        val payload = alloc.copyOf(byteArrayOf(0x10, 0x20, 0x30))
        val frame = RakNetFrame(reliability = Reliability.UNRELIABLE, payload = payload)
        val original = RakNetDatagram.Data(
            flags = PacketId.FLAG_VALID,
            sequenceNumber = 42,
            frames = listOf(frame),
        )

        val buf = alloc.allocate(128)
        RakNetDatagramCodec.encode(original, buf)
        val decoded = RakNetDatagramCodec.decode(buf, alloc)
        buf.close()

        assertTrue(decoded is RakNetDatagram.Data)
        val data = decoded
        assertEquals(42, data.sequenceNumber)
        assertEquals(1, data.frames.size)
        assertEquals(3, data.frames[0].payload.readableBytes())
        assertEquals(0x10, data.frames[0].payload.readByte().toInt() and 0xFF)
        data.release()
        payload.close()
    }

    @Test fun `Data datagram with multiple frames roundtrip`() {
        val frames = listOf(
            RakNetFrame(reliability = Reliability.UNRELIABLE, payload = alloc.copyOf(byteArrayOf(1))),
            RakNetFrame(reliability = Reliability.RELIABLE, reliableIndex = 7, payload = alloc.copyOf(byteArrayOf(2))),
            RakNetFrame(reliability = Reliability.RELIABLE_ORDERED, reliableIndex = 8, orderIndex = 0, orderChannel = 1, payload = alloc.copyOf(byteArrayOf(3))),
        )
        val original = RakNetDatagram.Data(flags = PacketId.FLAG_VALID, sequenceNumber = 99, frames = frames)

        val buf = alloc.allocate(256)
        RakNetDatagramCodec.encode(original, buf)
        val decoded = RakNetDatagramCodec.decode(buf, alloc)
        buf.close()

        assertTrue(decoded is RakNetDatagram.Data)
        val data = decoded
        assertEquals(99, data.sequenceNumber)
        assertEquals(3, data.frames.size)
        assertEquals(Reliability.RELIABLE, data.frames[1].reliability)
        assertEquals(7, data.frames[1].reliableIndex)
        data.release()
    }
}
