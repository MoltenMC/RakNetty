package io.github.agent0876.raknetty.handler

import io.github.agent0876.raknetty.codec.RakNetDatagramCodec
import io.github.agent0876.raknetty.codec.offline.OfflinePacketCodec
import io.github.agent0876.raknetty.core.exception.InvalidPacketException
import io.netty5.channel.ChannelHandlerContext
import io.netty5.channel.SimpleChannelInboundHandler
import io.netty5.channel.socket.DatagramPacket
import io.netty5.util.internal.logging.InternalLoggerFactory
import java.net.InetSocketAddress

class DatagramDispatcher(
    private val registry: ConnectionRegistry,
) : SimpleChannelInboundHandler<DatagramPacket>() {

    private val log = InternalLoggerFactory.getInstance(DatagramDispatcher::class.java)

    override fun messageReceived(ctx: ChannelHandlerContext, msg: DatagramPacket) {
        try {
            val buf    = msg.content()
            val sender = msg.sender() as InetSocketAddress

            if (buf.readableBytes() <= 0) return

            val firstByte = buf.getUnsignedByte(buf.readerOffset()).toInt()

            if (RakNetDatagramCodec.isOnlineDatagram(firstByte)) {
                val conn = registry.get(sender) ?: return
                try {
                    val datagram = RakNetDatagramCodec.decode(buf, ctx.bufferAllocator())
                    conn.onDatagramReceived(ctx, datagram)
                } catch (e: InvalidPacketException) {
                    log.warn("Invalid online datagram from {}: {}", sender, e.message)
                }
            } else {
                try {
                    val packet = OfflinePacketCodec.decode(buf) ?: return
                    ctx.fireChannelRead(AddressedOfflinePacket(packet, sender))
                } catch (e: InvalidPacketException) {
                    log.warn("Invalid offline packet from {}: {}", sender, e.message)
                }
            }
        } finally {
            msg.close()
        }
    }
}
