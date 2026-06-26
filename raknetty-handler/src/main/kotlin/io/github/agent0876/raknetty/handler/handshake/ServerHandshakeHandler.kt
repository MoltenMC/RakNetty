package io.github.agent0876.raknetty.handler.handshake

import io.github.agent0876.raknetty.codec.offline.OfflinePacket
import io.github.agent0876.raknetty.codec.offline.OfflinePacketCodec
import io.github.agent0876.raknetty.core.connection.ConnectionState
import io.github.agent0876.raknetty.core.protocol.RakNetProtocol
import io.github.agent0876.raknetty.handler.AddressedOfflinePacket
import io.github.agent0876.raknetty.handler.ConnectionRegistry
import io.github.agent0876.raknetty.handler.connection.ConnectionConfig
import io.github.agent0876.raknetty.handler.connection.RakNetConnectionImpl
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.SimpleChannelInboundHandler
import io.netty.channel.socket.DatagramPacket
import java.net.InetSocketAddress

/**
 * Server-side unconnected handshake state machine.
 *
 * Handles [AddressedOfflinePacket]s fired by [DatagramDispatcher]:
 * ```
 * Client → UnconnectedPing          → UnconnectedPong
 * Client → OpenConnectionRequest1   → OpenConnectionReply1  (or IncompatibleProtocolVersion)
 * Client → OpenConnectionRequest2   → OpenConnectionReply2  (creates RakNetConnectionImpl)
 * ```
 *
 * After a [RakNetConnectionImpl] is created, the [ConnectionRequest] /
 * [ConnectionRequestAccepted] exchange is handled inside [RakNetConnectionImpl]
 * via its reliability layer (it arrives as a reliable data frame, not an offline packet).
 */
class ServerHandshakeHandler(
    private val registry: ConnectionRegistry,
    private val serverGuid: Long,
    private val serverInfo: () -> String = { "" },
    private val config: ConnectionConfig = ConnectionConfig(),
    private val maxConnections: Int = 20,
) : SimpleChannelInboundHandler<AddressedOfflinePacket>(false) {

    override fun channelRead0(ctx: ChannelHandlerContext, msg: AddressedOfflinePacket) {
        when (val pkt = msg.packet) {
            is OfflinePacket.UnconnectedPing               -> handlePing(ctx, pkt, msg.sender)
            is OfflinePacket.OpenConnectionRequest1        -> handleOCReq1(ctx, pkt, msg.sender)
            is OfflinePacket.OpenConnectionRequest2        -> handleOCReq2(ctx, pkt, msg.sender)
            else -> ctx.fireChannelRead(msg)   // pass unrecognised offline packets up
        }
    }

    private fun handlePing(
        ctx: ChannelHandlerContext,
        ping: OfflinePacket.UnconnectedPing,
        sender: InetSocketAddress,
    ) {
        val pong = OfflinePacket.UnconnectedPong(ping.sendTimestamp, serverGuid, serverInfo())
        reply(ctx, pong, sender)
    }

    private fun handleOCReq1(
        ctx: ChannelHandlerContext,
        req: OfflinePacket.OpenConnectionRequest1,
        sender: InetSocketAddress,
    ) {
        if (req.protocolVersion != RakNetProtocol.VERSION) {
            reply(ctx, OfflinePacket.IncompatibleProtocolVersion(RakNetProtocol.VERSION, serverGuid), sender)
            return
        }
        reply(ctx, OfflinePacket.OpenConnectionReply1(serverGuid, useSecurity = false, mtu = req.mtu), sender)
    }

    private fun handleOCReq2(
        ctx: ChannelHandlerContext,
        req: OfflinePacket.OpenConnectionRequest2,
        sender: InetSocketAddress,
    ) {
        // If already connected, confirm reply (idempotent)
        val existing = registry.get(sender)
        if (existing != null) {
            reply(ctx, OfflinePacket.OpenConnectionReply2(serverGuid, sender, req.mtu, false), sender)
            return
        }
        if (registry.size >= maxConnections) {
            reply(ctx, OfflinePacket.NoFreeIncomingConnections(serverGuid), sender)
            return
        }

        val conn = RakNetConnectionImpl(
            remoteAddress = sender,
            guid          = req.clientGuid,
            mtu           = req.mtu,
            channel       = ctx.channel(),
            registry      = registry,
            config        = config,
        )
        conn.state = ConnectionState.HANDSHAKING
        registry.add(conn)
        conn.startTicker()

        reply(ctx, OfflinePacket.OpenConnectionReply2(serverGuid, sender, req.mtu, false), sender)
    }

    private fun reply(ctx: ChannelHandlerContext, packet: OfflinePacket, recipient: InetSocketAddress) {
        val buf = OfflinePacketCodec.encode(packet, ctx.alloc())
        ctx.writeAndFlush(DatagramPacket(buf, recipient))
    }
}
