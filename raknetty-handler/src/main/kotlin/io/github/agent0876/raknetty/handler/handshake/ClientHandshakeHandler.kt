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
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.SimpleChannelInboundHandler
import io.netty.channel.socket.DatagramPacket
import java.net.InetSocketAddress
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit

/**
 * Client-side handshake state machine.
 *
 * Call [connect] to initiate; this handler drives the full sequence:
 * ```
 * → OCReq1 (MTU probe)   ← OCReply1
 * → OCReq2               ← OCReply2
 * → ConnectionRequest    ← ConnectionRequestAccepted   (handled inside RakNetConnectionImpl)
 * → NewIncomingConnection                              (handled inside RakNetConnectionImpl)
 * ```
 *
 * OCReq1 is retried up to `MTU_SIZES.size - 1` times (one attempt per MTU probe),
 * spaced [ConnectionConfig.ocReq1RetryIntervalMs] apart. If the server replies with
 * OCReply1 before the next probe fires, the retry is cancelled immediately.
 */
class ClientHandshakeHandler(
    private val registry: ConnectionRegistry,
    private val clientGuid: Long,
    private val config: ConnectionConfig = ConnectionConfig(),
) : SimpleChannelInboundHandler<AddressedOfflinePacket>(false) {

    private var serverAddress: InetSocketAddress? = null
    private var probeIndex: Int = 0
    private var retryTask: Future<*>? = null

    override fun channelRead0(ctx: ChannelHandlerContext, msg: AddressedOfflinePacket) {
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

    /**
     * Initiates the connection to [server].
     *
     * [mtu] is the largest MTU to probe first; the handler steps down through
     * [RakNetProtocol.MTU_SIZES] automatically if no OCReply1 arrives within
     * [ConnectionConfig.ocReq1RetryIntervalMs]. Must be called from the EventLoop.
     */
    fun connect(ctx: ChannelHandlerContext, server: InetSocketAddress, mtu: Int = RakNetProtocol.MTU_SIZES[0]) {
        serverAddress = server
        // Start from the largest probe that does not exceed the requested MTU.
        probeIndex = RakNetProtocol.MTU_SIZES.indexOfFirst { it <= mtu }.coerceAtLeast(0)
        sendOCReq1(ctx, server)
        scheduleNextProbe(ctx, server)
    }

    // ── OCReq1 retry ──────────────────────────────────────────────────────────

    private fun sendOCReq1(ctx: ChannelHandlerContext, server: InetSocketAddress) {
        val mtu = RakNetProtocol.MTU_SIZES[probeIndex]
        val req = OfflinePacket.OpenConnectionRequest1(RakNetProtocol.VERSION, mtu)
        ctx.writeAndFlush(DatagramPacket(OfflinePacketCodec.encode(req, ctx.alloc()), server))
    }

    /**
     * Schedules a retry with the next smaller MTU probe, unless we are already
     * on the last probe. Everything runs on the EventLoop — no synchronisation needed.
     */
    private fun scheduleNextProbe(ctx: ChannelHandlerContext, server: InetSocketAddress) {
        val nextProbe = probeIndex + 1
        if (nextProbe >= RakNetProtocol.MTU_SIZES.size) return  // all probes sent

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
        retryTask?.cancel(false)
        retryTask = null
    }

    // ── Handshake steps ───────────────────────────────────────────────────────

    private fun handleOCReply1(
        ctx: ChannelHandlerContext,
        reply: OfflinePacket.OpenConnectionReply1,
        sender: InetSocketAddress,
    ) {
        if (sender != serverAddress) return
        cancelRetry()  // server replied — stop MTU probing

        val req = OfflinePacket.OpenConnectionRequest2(
            serverAddress = sender,
            mtu           = reply.mtu,
            clientGuid    = clientGuid,
        )
        ctx.writeAndFlush(DatagramPacket(OfflinePacketCodec.encode(req, ctx.alloc()), sender))
    }

    private fun handleOCReply2(
        ctx: ChannelHandlerContext,
        reply: OfflinePacket.OpenConnectionReply2,
        sender: InetSocketAddress,
    ) {
        if (sender != serverAddress || registry.get(sender) != null) return

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
        val buf = ConnectedPacketCodec.encode(connReq, ctx.alloc())
        conn.send(buf, Reliability.RELIABLE_ORDERED, 0)
    }
}
