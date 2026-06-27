package io.github.moltenmc.raknetty.codec.connected

import io.github.moltenmc.raknetty.codec.ext.UNASSIGNED_ADDRESS
import io.github.moltenmc.raknetty.codec.ext.writeAddress
import io.github.moltenmc.raknetty.core.protocol.PacketId
import io.netty5.buffer.BufferAllocator
import java.net.InetSocketAddress
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class ConnectedPacketCodecTest {

    private val alloc = BufferAllocator.onHeapUnpooled()

    private fun roundtrip(packet: ConnectedPacket): ConnectedPacket {
        val encoded = ConnectedPacketCodec.encode(packet, alloc)
        return ConnectedPacketCodec.decode(encoded)
    }

    @Test fun `ConnectionRequest roundtrip`() {
        val original = ConnectedPacket.ConnectionRequest(
            clientGuid = 0xCAFEBABE_DEADL,
            requestTimestamp = 1000L,
            doSecurity = false,
        )
        val decoded = roundtrip(original) as ConnectedPacket.ConnectionRequest
        assertEquals(original.clientGuid, decoded.clientGuid)
        assertEquals(original.requestTimestamp, decoded.requestTimestamp)
        assertEquals(original.doSecurity, decoded.doSecurity)
    }

    @Test fun `ConnectionRequestAccepted roundtrip`() {
        val addresses = List(ConnectedPacket.INTERNAL_ADDRESS_COUNT) { i ->
            if (i == 0) InetSocketAddress("192.168.1.100", 19132)
            else UNASSIGNED_ADDRESS
        }
        val original = ConnectedPacket.ConnectionRequestAccepted(
            clientAddress = InetSocketAddress("10.0.0.5", 50000),
            clientIndex = 0,
            systemAddresses = addresses,
            requestTimestamp = 2000L,
            acceptedTimestamp = 2500L,
        )
        val decoded = roundtrip(original) as ConnectedPacket.ConnectionRequestAccepted
        assertEquals(original.clientAddress, decoded.clientAddress)
        assertEquals(original.clientIndex, decoded.clientIndex)
        assertEquals(original.systemAddresses.size, decoded.systemAddresses.size)
        assertEquals(original.systemAddresses[0], decoded.systemAddresses[0])
        assertEquals(original.requestTimestamp, decoded.requestTimestamp)
        assertEquals(original.acceptedTimestamp, decoded.acceptedTimestamp)
    }

    @Test fun `NewIncomingConnection roundtrip`() {
        val internal = List(ConnectedPacket.INTERNAL_ADDRESS_COUNT) {
            InetSocketAddress("192.168.1.1", 19132)
        }
        val original = ConnectedPacket.NewIncomingConnection(
            serverAddress = InetSocketAddress("203.0.113.50", 19132),
            internalAddresses = internal,
            requestTimestamp = 3000L,
            acceptedTimestamp = 3500L,
        )
        val decoded = roundtrip(original) as ConnectedPacket.NewIncomingConnection
        assertEquals(original.serverAddress, decoded.serverAddress)
        assertEquals(original.internalAddresses.size, decoded.internalAddresses.size)
        assertEquals(original.internalAddresses[0], decoded.internalAddresses[0])
        assertEquals(original.requestTimestamp, decoded.requestTimestamp)
        assertEquals(original.acceptedTimestamp, decoded.acceptedTimestamp)
    }

    @Test fun `DisconnectionNotification roundtrip`() {
        val decoded = roundtrip(ConnectedPacket.DisconnectionNotification)
        assertTrue(decoded is ConnectedPacket.DisconnectionNotification)
    }

    @Test fun `DetectLostConnection roundtrip`() {
        val decoded = roundtrip(ConnectedPacket.DetectLostConnection)
        assertTrue(decoded is ConnectedPacket.DetectLostConnection)
    }

    @Test fun `ConnectedPing roundtrip`() {
        val original = ConnectedPacket.ConnectedPing(sendTimestamp = 5000L)
        val decoded = roundtrip(original) as ConnectedPacket.ConnectedPing
        assertEquals(5000L, decoded.sendTimestamp)
    }

    @Test fun `ConnectedPong roundtrip`() {
        val original = ConnectedPacket.ConnectedPong(sendTimestamp = 6000L, pingTimestamp = 5500L)
        val decoded = roundtrip(original) as ConnectedPacket.ConnectedPong
        assertEquals(6000L, decoded.sendTimestamp)
        assertEquals(5500L, decoded.pingTimestamp)
    }

    @Test fun `decode with unknown ID throws`() {
        val buf = alloc.allocate(1)
        buf.writeByte(0xFE.toByte())
        assertFailsWith<io.github.moltenmc.raknetty.core.exception.InvalidPacketException> {
            ConnectedPacketCodec.decode(buf)
        }
        buf.close()
    }
}
