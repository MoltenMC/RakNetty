package io.github.agent0876.raknetty.handler

import io.github.agent0876.raknetty.codec.RakNetDatagramCodec
import io.github.agent0876.raknetty.codec.offline.OfflinePacketCodec
import io.github.agent0876.raknetty.core.exception.InvalidPacketException
import io.netty.channel.ChannelHandler
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.SimpleChannelInboundHandler
import io.netty.channel.socket.DatagramPacket
import java.net.InetSocketAddress

/**
 * First inbound handler on the [NioDatagramChannel] pipeline.
 *
 * Routes each UDP packet to one of two paths:
 * - **Online** (first byte has FLAG_VALID = 0x80): decoded as [RakNetDatagram] and
 *   dispatched directly to the owning [RakNetConnectionImpl] via [ConnectionRegistry].
 * - **Offline** (first byte < 0x80): decoded as [OfflinePacket] and fired up the
 *   pipeline as [AddressedOfflinePacket] for the handshake handler to process.
 *
 * Malformed packets and unknown offline IDs are silently dropped.
 */
@ChannelHandler.Sharable
class DatagramDispatcher(
    private val registry: ConnectionRegistry,
) : SimpleChannelInboundHandler<DatagramPacket>(false) {

    override fun channelRead0(ctx: ChannelHandlerContext, msg: DatagramPacket) {
        try {
            val buf    = msg.content()
            val sender = msg.sender() as InetSocketAddress

            if (!buf.isReadable) return

            val firstByte = buf.getUnsignedByte(buf.readerIndex()).toInt()

            if (RakNetDatagramCodec.isOnlineDatagram(firstByte)) {
                val conn = registry.get(sender) ?: return
                try {
                    val datagram = RakNetDatagramCodec.decode(buf, ctx.alloc())
                    conn.onDatagramReceived(ctx, datagram)
                } catch (_: InvalidPacketException) { /* drop malformed datagram */ }
            } else {
                try {
                    val packet = OfflinePacketCodec.decode(buf) ?: return
                    ctx.fireChannelRead(AddressedOfflinePacket(packet, sender))
                } catch (_: InvalidPacketException) { /* drop unknown offline packet */ }
            }
        } finally {
            msg.release()
        }
    }
}
