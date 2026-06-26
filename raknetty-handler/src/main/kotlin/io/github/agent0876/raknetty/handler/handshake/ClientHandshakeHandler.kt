package io.github.agent0876.raknetty.handler.handshake

import io.github.agent0876.raknetty.codec.connected.ConnectedPacket
import io.github.agent0876.raknetty.codec.connected.ConnectedPacketCodec
import io.github.agent0876.raknetty.codec.offline.OfflinePacket
import io.github.agent0876.raknetty.codec.offline.OfflinePacketCodec
import io.github.agent0876.raknetty.core.connection.ConnectionState
import io.github.agent0876.raknetty.core.protocol.RakNetProtocol
import io.github.agent0876.raknetty.core.protocol.Reliability
import io.github.agent0876.raknetty.handler.AddressedOfflinePacket
import io.github.agent0876.raknetty.handler.ConnectionRegistry
import io.github.agent0876.raknetty.handler.connection.ConnectionConfig
import io.github.agent0876.raknetty.handler.connection.RakNetConnectionImpl
import io.netty5.channel.ChannelHandlerContext
import io.netty5.channel.SimpleChannelInboundHandler
import io.netty5.channel.socket.DatagramPacket
import io.netty5.util.concurrent.Future
import io.netty5.util.internal.logging.InternalLoggerFactory
import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit

class ClientHandshakeHandler(
    private val registry: ConnectionRegistry,
    private val clientGuid: Long,
    private val config: ConnectionConfig = ConnectionConfig(),
) : SimpleChannelInboundHandler<AddressedOfflinePacket>() {

    private val log = InternalLoggerFactory.getInstance(ClientHandshakeHandler::class.java)
    private var serverAddress: InetSocketAddress? = null
    private var probeIndex: Int = 0
    private var retryTask: Future<Void>? = null

    override fun messageReceived(ctx: ChannelHandlerContext, msg: AddressedOfflinePacket) {
        when (val pkt = msg.packet) {
            is OfflinePacket.OpenConnectionReply1 -> handleOCReply1(ctx, pkt, msg.sender)
            is OfflinePacket.OpenConnectionReply2 -> handleOCReply2(ctx, pkt, msg.sender)
            is OfflinePacket.IncompatibleProtocolVersion,
            is OfflinePacket.NoFreeIncomingConnections,
            is OfflinePacket.ConnectionBanned,
            is OfflinePacket.AlreadyConnected     -> ctx.fireChannelRead(msg)
            else -> ctx.fireChannelRead(msg)
        }
    }

    fun connect(ctx: ChannelHandlerContext, server: InetSocketAddress, mtu: Int = RakNetProtocol.MTU_SIZES[0]) {
        serverAddress = server
        probeIndex = RakNetProtocol.MTU_SIZES.indexOfFirst { it <= mtu }.coerceAtLeast(0)
        log.debug("Connecting to {} with initial MTU={} (probeIndex={})", server, RakNetProtocol.MTU_SIZES[probeIndex], probeIndex)
        sendOCReq1(ctx, server)
        scheduleNextProbe(ctx, server)
    }

    private fun sendOCReq1(ctx: ChannelHandlerContext, server: InetSocketAddress) {
        val mtu = RakNetProtocol.MTU_SIZES[probeIndex]
        val req = OfflinePacket.OpenConnectionRequest1(RakNetProtocol.VERSION, mtu)
        ctx.writeAndFlush(DatagramPacket(OfflinePacketCodec.encode(req, ctx.bufferAllocator()), server))
    }

    private fun scheduleNextProbe(ctx: ChannelHandlerContext, server: InetSocketAddress) {
        val nextProbe = probeIndex + 1
        if (nextProbe >= RakNetProtocol.MTU_SIZES.size) return

        retryTask = ctx.executor().schedule(
            {
                probeIndex = nextProbe
                sendOCReq1(ctx, server)
                scheduleNextProbe(ctx, server)
            },
            config.ocReq1RetryIntervalMs,
            TimeUnit.MILLISECONDS,
        )
    }

    private fun cancelRetry() {
        retryTask?.cancel()
        retryTask = null
    }

    private fun handleOCReply1(
        ctx: ChannelHandlerContext,
        reply: OfflinePacket.OpenConnectionReply1,
        sender: InetSocketAddress,
    ) {
        if (sender != serverAddress) {
            log.debug("Ignoring OCReply1 from unexpected sender {}", sender)
            return
        }
        log.debug("OCReply1 received (MTU={})", reply.mtu)
        cancelRetry()

        val req = OfflinePacket.OpenConnectionRequest2(
            serverAddress = sender,
            mtu           = reply.mtu,
            clientGuid    = clientGuid,
        )
        ctx.writeAndFlush(DatagramPacket(OfflinePacketCodec.encode(req, ctx.bufferAllocator()), sender))
    }

    private fun handleOCReply2(
        ctx: ChannelHandlerContext,
        reply: OfflinePacket.OpenConnectionReply2,
        sender: InetSocketAddress,
    ) {
        if (sender != serverAddress) {
            log.debug("Ignoring OCReply2 from unexpected sender {}", sender)
            return
        }
        if (registry.get(sender) != null) {
            log.debug("OCReply2 from {} ignored: already registered", sender)
            return
        }
        log.debug("OCReply2 received from {} (MTU={}, serverGUID={})", sender, reply.mtu, reply.serverGuid)

        val conn = RakNetConnectionImpl(
            remoteAddress = sender,
            guid          = reply.serverGuid,
            mtu           = reply.mtu,
            channel       = ctx.channel(),
            registry      = registry,
            config        = config,
        )
        conn.state = ConnectionState.HANDSHAKING
        registry.add(conn)
        conn.startTicker()

        val connReq = ConnectedPacket.ConnectionRequest(
            clientGuid       = clientGuid,
            requestTimestamp = System.currentTimeMillis(),
            doSecurity       = false,
        )
        val buf = ConnectedPacketCodec.encode(connReq, ctx.bufferAllocator())
        conn.send(buf, Reliability.RELIABLE_ORDERED, 0)
    }
}
