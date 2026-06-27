package io.github.moltenmc.raknetty.handler.handshake

import io.github.moltenmc.raknetty.codec.offline.OfflinePacket
import io.github.moltenmc.raknetty.codec.offline.OfflinePacketCodec
import io.github.moltenmc.raknetty.core.connection.ConnectionState
import io.github.moltenmc.raknetty.core.protocol.RakNetProtocol
import io.github.moltenmc.raknetty.handler.AddressedOfflinePacket
import io.github.moltenmc.raknetty.handler.ConnectionRegistry
import io.github.moltenmc.raknetty.handler.connection.ConnectionConfig
import io.github.moltenmc.raknetty.handler.connection.RakNetConnectionImpl
import io.netty5.channel.ChannelHandlerContext
import io.netty5.channel.SimpleChannelInboundHandler
import io.netty5.channel.socket.DatagramPacket
import io.netty5.util.internal.logging.InternalLoggerFactory
import java.net.InetSocketAddress

class ServerHandshakeHandler(
    private val registry: ConnectionRegistry,
    private val serverGuid: Long,
    private val serverInfo: () -> String = { "" },
    private val config: ConnectionConfig = ConnectionConfig(),
    private val maxConnections: Int = 20,
) : SimpleChannelInboundHandler<AddressedOfflinePacket>() {

    private val log = InternalLoggerFactory.getInstance(ServerHandshakeHandler::class.java)

    override fun messageReceived(ctx: ChannelHandlerContext, msg: AddressedOfflinePacket) {
        when (val pkt = msg.packet) {
            is OfflinePacket.UnconnectedPing               -> handlePing(ctx, pkt, msg.sender)
            is OfflinePacket.OpenConnectionRequest1        -> handleOCReq1(ctx, pkt, msg.sender)
            is OfflinePacket.OpenConnectionRequest2        -> handleOCReq2(ctx, pkt, msg.sender)
            else -> ctx.fireChannelRead(msg)
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
            log.warn("OCReq1 from {} rejected: protocol {} != {}", sender, req.protocolVersion, RakNetProtocol.VERSION)
            reply(ctx, OfflinePacket.IncompatibleProtocolVersion(RakNetProtocol.VERSION, serverGuid), sender)
            return
        }
        log.debug("OCReq1 from {} accepted (MTU={})", sender, req.mtu)
        reply(ctx, OfflinePacket.OpenConnectionReply1(serverGuid, useSecurity = false, mtu = req.mtu), sender)
    }

    private fun handleOCReq2(
        ctx: ChannelHandlerContext,
        req: OfflinePacket.OpenConnectionRequest2,
        sender: InetSocketAddress,
    ) {
        val existing = registry.get(sender)
        if (existing != null) {
            log.debug("OCReq2 from {}: already connected, resending reply", sender)
            reply(ctx, OfflinePacket.OpenConnectionReply2(serverGuid, sender, req.mtu, false), sender)
            return
        }
        if (registry.size >= maxConnections) {
            log.warn("OCReq2 from {} rejected: server full ({}/{}))", sender, registry.size, maxConnections)
            reply(ctx, OfflinePacket.NoFreeIncomingConnections(serverGuid), sender)
            return
        }

        log.debug("OCReq2 from {} accepted (MTU={})", sender, req.mtu)
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
        val buf = OfflinePacketCodec.encode(packet, ctx.bufferAllocator())
        ctx.writeAndFlush(DatagramPacket(buf, recipient))
    }
}
