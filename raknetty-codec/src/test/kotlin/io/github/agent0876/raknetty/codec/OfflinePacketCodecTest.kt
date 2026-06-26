package io.github.agent0876.raknetty.codec

import io.github.agent0876.raknetty.codec.offline.OfflinePacket
import io.github.agent0876.raknetty.codec.offline.OfflinePacketCodec
import io.github.agent0876.raknetty.core.protocol.RakNetProtocol
import io.netty.buffer.Unpooled
import java.net.InetSocketAddress
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class OfflinePacketCodecTest {

    private val alloc = Unpooled.EMPTY_BUFFER.alloc()

    private inline fun <reified T : OfflinePacket> roundtrip(packet: OfflinePacket): T {
        val encoded = OfflinePacketCodec.encode(packet, alloc)
        val decoded = OfflinePacketCodec.decode(encoded)
        encoded.release()
        return decoded as T
    }

    @Test fun `UnconnectedPing roundtrip`() {
        val original = OfflinePacket.UnconnectedPing(sendTimestamp = 12345L, senderGuid = 0xDEADBEEFL)
        val decoded  = roundtrip<OfflinePacket.UnconnectedPing>(original)
        assertEquals(original, decoded)
    }

    @Test fun `UnconnectedPong roundtrip`() {
        val original = OfflinePacket.UnconnectedPong(9999L, 42L, "MCPE;Test;100;1.0;0;10")
        val decoded  = roundtrip<OfflinePacket.UnconnectedPong>(original)
        assertEquals(original, decoded)
    }

    @Test fun `OpenConnectionRequest1 roundtrip preserves MTU`() {
        val mtu = 1492
        val original = OfflinePacket.OpenConnectionRequest1(RakNetProtocol.VERSION, mtu)
        val encoded  = OfflinePacketCodec.encode(original, alloc)
        val decoded  = OfflinePacketCodec.decode(encoded) as OfflinePacket.OpenConnectionRequest1
        encoded.release()

        assertEquals(RakNetProtocol.VERSION, decoded.protocolVersion)
        assertEquals(mtu, decoded.mtu)
    }

    @Test fun `OpenConnectionReply1 roundtrip`() {
        val original = OfflinePacket.OpenConnectionReply1(serverGuid = 1L, useSecurity = false, mtu = 1492)
        val decoded  = roundtrip<OfflinePacket.OpenConnectionReply1>(original)
        assertEquals(original, decoded)
    }

    @Test fun `OpenConnectionRequest2 roundtrip`() {
        val addr = InetSocketAddress("127.0.0.1", 19132)
        val original = OfflinePacket.OpenConnectionRequest2(addr, mtu = 1492, clientGuid = 99L)
        val decoded  = roundtrip<OfflinePacket.OpenConnectionRequest2>(original)
        assertEquals(original.mtu,        decoded.mtu)
        assertEquals(original.clientGuid, decoded.clientGuid)
        assertEquals(addr.port,           decoded.serverAddress.port)
    }

    @Test fun `OpenConnectionReply2 roundtrip`() {
        val clientAddr = InetSocketAddress("192.168.1.5", 12345)
        val original = OfflinePacket.OpenConnectionReply2(77L, clientAddr, 1200, false)
        val decoded  = roundtrip<OfflinePacket.OpenConnectionReply2>(original)
        assertEquals(original.serverGuid,        decoded.serverGuid)
        assertEquals(original.mtu,               decoded.mtu)
        assertEquals(original.encryptionEnabled, decoded.encryptionEnabled)
    }

    @Test fun `invalid magic returns null`() {
        // craft a ping with corrupted magic
        val buf = alloc.buffer()
        buf.writeByte(0x01) // UNCONNECTED_PING id
        buf.writeLong(0)    // timestamp
        buf.writeZero(16)   // wrong magic (all zeros)
        buf.writeLong(0)    // guid
        val result = OfflinePacketCodec.decode(buf)
        buf.release()
        assertNull(result)
    }
}
