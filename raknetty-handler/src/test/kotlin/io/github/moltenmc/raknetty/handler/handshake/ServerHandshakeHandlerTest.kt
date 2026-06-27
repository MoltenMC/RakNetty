package io.github.moltenmc.raknetty.handler.handshake

import io.github.moltenmc.raknetty.codec.offline.OfflinePacket
import io.github.moltenmc.raknetty.codec.offline.OfflinePacketCodec
import io.github.moltenmc.raknetty.core.connection.ConnectionState
import io.github.moltenmc.raknetty.core.protocol.RakNetProtocol
import io.github.moltenmc.raknetty.handler.AddressedOfflinePacket
import io.github.moltenmc.raknetty.handler.ConnectionRegistry
import io.github.moltenmc.raknetty.handler.connection.ConnectionConfig
import io.netty5.buffer.BufferAllocator
import io.netty5.channel.embedded.EmbeddedChannel
import io.netty5.channel.socket.DatagramPacket
import java.net.InetSocketAddress
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class ServerHandshakeHandlerTest {

    private val alloc = BufferAllocator.onHeapUnpooled()
    private val sender = InetSocketAddress("192.168.1.100", 50000)

    private fun handler(
        registry: ConnectionRegistry = ConnectionRegistry(),
        maxConnections: Int = 20,
    ) = ServerHandshakeHandler(
        registry = registry,
        serverGuid = 42L,
        serverInfo = { "Test Server" },
        config = ConnectionConfig(),
        maxConnections = maxConnections,
    )

    private fun readResponse(ch: EmbeddedChannel): DatagramPacket {
        ch.flushOutbound()
        val msg = ch.readOutbound<DatagramPacket>()
        assertNotNull(msg) { "Expected outbound DatagramPacket" }
        return msg
    }

    @Test fun `ping receives pong with correct server info`() {
        val h = handler()
        val ch = EmbeddedChannel(h)

        val ping = OfflinePacket.UnconnectedPing(sendTimestamp = 1000L, senderGuid = 99L)
        ch.writeInbound(AddressedOfflinePacket(ping, sender))

        val response = readResponse(ch)
        val pong = OfflinePacketCodec.decode(response.content()) as OfflinePacket.UnconnectedPong
        assertEquals(1000L, pong.sendTimestamp)
        assertEquals(42L, pong.serverGuid)
        assertEquals("Test Server", pong.serverInfo)
    }

    @Test fun `OCReq1 accepted when version matches`() {
        val h = handler()
        val ch = EmbeddedChannel(h)

        val req = OfflinePacket.OpenConnectionRequest1(RakNetProtocol.VERSION, 1492)
        ch.writeInbound(AddressedOfflinePacket(req, sender))

        val response = readResponse(ch)
        val reply = OfflinePacketCodec.decode(response.content()) as OfflinePacket.OpenConnectionReply1
        assertEquals(42L, reply.serverGuid)
        assertEquals(1492, reply.mtu)
    }

    @Test fun `OCReq1 rejected when version mismatches`() {
        val h = handler()
        val ch = EmbeddedChannel(h)

        val req = OfflinePacket.OpenConnectionRequest1(99, 1492)
        ch.writeInbound(AddressedOfflinePacket(req, sender))

        val response = readResponse(ch)
        val reply = OfflinePacketCodec.decode(response.content()) as OfflinePacket.IncompatibleProtocolVersion
        assertEquals(RakNetProtocol.VERSION, reply.protocolVersion)
    }

    @Test fun `OCReq2 creates connection and replies`() {
        val reg = ConnectionRegistry()
        val h = handler(registry = reg)
        val ch = EmbeddedChannel(h)

        val req = OfflinePacket.OpenConnectionRequest2(
            serverAddress = sender,
            mtu = 1200,
            clientGuid = 99L,
        )
        ch.writeInbound(AddressedOfflinePacket(req, sender))

        val conn = reg.get(sender)
        assertNotNull(conn) { "Connection should be registered" }
        assertEquals(ConnectionState.HANDSHAKING, conn.state)
        assertEquals(99L, conn.guid)

        val response = readResponse(ch)
        val reply = OfflinePacketCodec.decode(response.content()) as OfflinePacket.OpenConnectionReply2
        assertEquals(42L, reply.serverGuid)
        assertEquals(1200, reply.mtu)
    }

    @Test fun `OCReq2 resent for already connected client`() {
        val reg = ConnectionRegistry()
        val h = handler(registry = reg)
        val ch = EmbeddedChannel(h)

        val req1 = OfflinePacket.OpenConnectionRequest2(sender, 1492, 99L)
        ch.writeInbound(AddressedOfflinePacket(req1, sender))
        readResponse(ch)

        val req2 = OfflinePacket.OpenConnectionRequest2(sender, 1492, 99L)
        ch.writeInbound(AddressedOfflinePacket(req2, sender))

        val response = readResponse(ch)
        val reply = OfflinePacketCodec.decode(response.content()) as OfflinePacket.OpenConnectionReply2
        assertEquals(42L, reply.serverGuid)
    }

    @Test fun `OCReq2 rejected when server is full`() {
        val reg = ConnectionRegistry()
        val h = handler(registry = reg, maxConnections = 0)
        val ch = EmbeddedChannel(h)

        val req = OfflinePacket.OpenConnectionRequest2(sender, 1492, 99L)
        ch.writeInbound(AddressedOfflinePacket(req, sender))

        val response = readResponse(ch)
        val reply = OfflinePacketCodec.decode(response.content()) as OfflinePacket.NoFreeIncomingConnections
        assertEquals(42L, reply.serverGuid)
    }

    @Test fun `unknown packet is forwarded`() {
        val h = handler()
        val ch = EmbeddedChannel(h)

        val pong = OfflinePacket.UnconnectedPong(1000L, 42L, "info")
        ch.writeInbound(AddressedOfflinePacket(pong, sender))

        assertNull(ch.readOutbound<Any>(), "no outbound should be written")
    }
}
