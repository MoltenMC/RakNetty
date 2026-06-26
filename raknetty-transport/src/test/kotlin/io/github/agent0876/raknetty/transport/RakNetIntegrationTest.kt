package io.github.agent0876.raknetty.transport

import io.github.agent0876.raknetty.core.connection.DisconnectReason
import io.github.agent0876.raknetty.core.connection.RakNetConnection
import io.github.agent0876.raknetty.core.protocol.Reliability
import io.netty5.buffer.BufferAllocator
import java.nio.ByteBuffer
import io.netty5.channel.ChannelHandlerContext
import io.netty5.channel.EventLoopGroup
import io.netty5.channel.MultithreadEventLoopGroup
import io.netty5.channel.nio.NioHandler
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

    @BeforeEach fun setUp()    { group = MultithreadEventLoopGroup(2, NioHandler.newFactory()) }
    @AfterEach  fun tearDown() { group.shutdownGracefully().asStage().sync() }

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
            .asStage().sync()

        val serverChannel = serverFuture.getNow()
        val serverPort = (serverChannel.localAddress() as InetSocketAddress).port

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

        assertTrue(connectFuture.asStage().await(5, TimeUnit.SECONDS),  "Client connect timed out")
        assertTrue(serverConnected.await(2, TimeUnit.SECONDS), "Server did not see connection")
        assertTrue(clientConnected.await(2, TimeUnit.SECONDS), "Client did not see connection")

        assertNotNull(serverConn.get())
        assertNotNull(clientConn.get())
        assertEquals(serverConn.get()!!.mtu, clientConn.get()!!.mtu)

        serverChannel.close().asStage().sync()
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
                    payload: io.netty5.buffer.Buffer,
                ) {
                    val bytes = ByteArray(payload.readableBytes())
                    payload.readBytes(ByteBuffer.wrap(bytes))
                    receivedBytes.set(bytes)
                    messageReceived.countDown()
                }
            })
            .bind(0).asStage().sync()

        val serverChannel2 = serverFuture.getNow()
        val serverPort = (serverChannel2.localAddress() as InetSocketAddress).port

        val connectFuture = RakNetClientBootstrap()
            .group(group)
            .clientGuid(20L)
            .handler(object : SimpleRakNetHandler() {
                override fun onConnect(ctx: ChannelHandlerContext, connection: RakNetConnection) {
                    val payload = BufferAllocator.onHeapUnpooled().allocate(5)
                    payload.writeByte(0x80.toByte())
                    payload.writeInt(0xCAFEBABE.toInt())
                    connection.send(payload, Reliability.RELIABLE_ORDERED)
                }
            })
            .connect("127.0.0.1", serverPort)

        assertTrue(connectFuture.asStage().await(5, TimeUnit.SECONDS),     "Connect timed out")
        assertTrue(messageReceived.await(3, TimeUnit.SECONDS),    "Message not received")

        assertNotNull(receivedBytes.get())
        assertEquals(5, receivedBytes.get()!!.size)
        assertEquals(0x80.toByte(), receivedBytes.get()!![0])

        serverChannel2.close().asStage().sync()
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
            .bind(0).asStage().sync()

        val serverChannel3 = serverFuture.getNow()
        val serverPort = (serverChannel3.localAddress() as InetSocketAddress).port

        val connectFuture = RakNetClientBootstrap()
            .group(group)
            .clientGuid(200L)
            .handler(object : SimpleRakNetHandler() {
                override fun onConnect(ctx: ChannelHandlerContext, connection: RakNetConnection) {
                    connection.disconnect()
                }
            })
            .connect("127.0.0.1", serverPort)

        assertTrue(connectFuture.asStage().await(5, TimeUnit.SECONDS), "Connect timed out")
        assertTrue(serverDisconnected.await(3, TimeUnit.SECONDS), "Server did not see disconnect")
        assertEquals(DisconnectReason.CLIENT_REQUESTED, disconnectReason.get())

        serverChannel3.close().asStage().sync()
    }
}
