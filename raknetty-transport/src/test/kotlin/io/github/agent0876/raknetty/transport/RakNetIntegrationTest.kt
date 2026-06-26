package io.github.agent0876.raknetty.transport

import io.github.agent0876.raknetty.core.connection.DisconnectReason
import io.github.agent0876.raknetty.core.connection.RakNetConnection
import io.github.agent0876.raknetty.core.protocol.Reliability
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.EventLoopGroup
import io.netty.channel.MultiThreadIoEventLoopGroup
import io.netty.channel.nio.NioIoHandler
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import java.net.InetSocketAddress
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class RakNetIntegrationTest {

    private lateinit var group: EventLoopGroup

    @BeforeEach fun setUp()    { group = MultiThreadIoEventLoopGroup(2, NioIoHandler.newFactory()) }
    @AfterEach  fun tearDown() { group.shutdownGracefully().sync() }

    @Test fun `server and client complete handshake`() {
        val serverConnected = CountDownLatch(1)
        val clientConnected = CountDownLatch(1)
        val serverConn      = AtomicReference<RakNetConnection>()
        val clientConn      = AtomicReference<RakNetConnection>()

        // ── Server ────────────────────────────────────────────────────────────
        val serverFuture = RakNetServerBootstrap()
            .group(group)
            .serverGuid(1L)
            .serverInfo { "RakNetty Test" }
            .handler(object : SimpleRakNetHandler() {
                override fun onConnect(ctx: ChannelHandlerContext, connection: RakNetConnection) {
                    serverConn.set(connection)
                    serverConnected.countDown()
                }
            })
            .bind(0)
            .sync()

        val serverPort = (serverFuture.channel().localAddress() as InetSocketAddress).port

        // ── Client ────────────────────────────────────────────────────────────
        val connectFuture = RakNetClientBootstrap()
            .group(group)
            .clientGuid(2L)
            .handler(object : SimpleRakNetHandler() {
                override fun onConnect(ctx: ChannelHandlerContext, connection: RakNetConnection) {
                    clientConn.set(connection)
                    clientConnected.countDown()
                }
            })
            .connect("127.0.0.1", serverPort)

        assertTrue(connectFuture.await(5, TimeUnit.SECONDS),  "Client connect timed out")
        assertTrue(serverConnected.await(2, TimeUnit.SECONDS), "Server did not see connection")
        assertTrue(clientConnected.await(2, TimeUnit.SECONDS), "Client did not see connection")

        assertNotNull(serverConn.get())
        assertNotNull(clientConn.get())
        assertEquals(serverConn.get()!!.mtu, clientConn.get()!!.mtu)

        serverFuture.channel().close().sync()
    }

    @Test fun `client can send message to server after handshake`() {
        val messageReceived = CountDownLatch(1)
        val receivedBytes   = AtomicReference<ByteArray>()

        val serverFuture = RakNetServerBootstrap()
            .group(group)
            .serverGuid(10L)
            .handler(object : SimpleRakNetHandler() {
                override fun onMessage(
                    ctx: ChannelHandlerContext,
                    connection: RakNetConnection,
                    payload: io.netty.buffer.ByteBuf,
                ) {
                    val bytes = ByteArray(payload.readableBytes())
                    payload.readBytes(bytes)
                    receivedBytes.set(bytes)
                    messageReceived.countDown()
                }
            })
            .bind(0).sync()

        val serverPort = (serverFuture.channel().localAddress() as InetSocketAddress).port

        val connectFuture = RakNetClientBootstrap()
            .group(group)
            .clientGuid(20L)
            .handler(object : SimpleRakNetHandler() {
                override fun onConnect(ctx: ChannelHandlerContext, connection: RakNetConnection) {
                    // 0x80 prefix = application-layer data marker
                    val payload = Unpooled.buffer(5)
                        .writeByte(0x80)
                        .writeInt(0xCAFEBABE.toInt())
                    connection.send(payload, Reliability.RELIABLE_ORDERED)
                }
            })
            .connect("127.0.0.1", serverPort)

        assertTrue(connectFuture.await(5, TimeUnit.SECONDS),     "Connect timed out")
        assertTrue(messageReceived.await(3, TimeUnit.SECONDS),    "Message not received")

        assertNotNull(receivedBytes.get())
        assertEquals(5, receivedBytes.get()!!.size)
        assertEquals(0x80.toByte(), receivedBytes.get()!![0])

        serverFuture.channel().close().sync()
    }

    @Test fun `client disconnect fires server Disconnected event`() {
        val serverDisconnected = CountDownLatch(1)
        val disconnectReason   = AtomicReference<DisconnectReason>()

        val serverFuture = RakNetServerBootstrap()
            .group(group)
            .serverGuid(100L)
            .handler(object : SimpleRakNetHandler() {
                override fun onDisconnect(
                    ctx: ChannelHandlerContext,
                    connection: RakNetConnection,
                    reason: DisconnectReason,
                ) {
                    disconnectReason.set(reason)
                    serverDisconnected.countDown()
                }
            })
            .bind(0).sync()

        val serverPort = (serverFuture.channel().localAddress() as InetSocketAddress).port

        val connectFuture = RakNetClientBootstrap()
            .group(group)
            .clientGuid(200L)
            .handler(object : SimpleRakNetHandler() {
                override fun onConnect(ctx: ChannelHandlerContext, connection: RakNetConnection) {
                    connection.disconnect()
                }
            })
            .connect("127.0.0.1", serverPort)

        assertTrue(connectFuture.await(5, TimeUnit.SECONDS), "Connect timed out")
        assertTrue(serverDisconnected.await(3, TimeUnit.SECONDS), "Server did not see disconnect")
        assertEquals(DisconnectReason.CLIENT_REQUESTED, disconnectReason.get())

        serverFuture.channel().close().sync()
    }
}
