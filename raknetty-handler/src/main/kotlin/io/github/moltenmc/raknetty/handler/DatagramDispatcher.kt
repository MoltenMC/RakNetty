package io.github.moltenmc.raknetty.handler

import io.github.moltenmc.raknetty.codec.RakNetDatagramCodec
import io.github.moltenmc.raknetty.codec.offline.OfflinePacketCodec
import io.github.moltenmc.raknetty.core.exception.InvalidPacketException
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
        val buf    = msg.content()
        val sender = msg.sender() as InetSocketAddress

        if (buf.readableBytes() <= 0) return

        val firstByte = buf.getUnsignedByte(buf.readerOffset()).toInt()

        if (RakNetDatagramCodec.isOnlineDatagram(firstByte)) {
            val conn = registry.get(sender) ?: return
            try {
                val datagram = RakNetDatagramCodec.decode(buf, ctx.bufferAllocator())
                conn.onDatagramReceived(ctx, datagram)
            } catch (e: Exception) {
                log.warn("Failed to decode or process online datagram from {}: {}", sender, e.message, e)
            }
        } else {
            try {
                val packet = OfflinePacketCodec.decode(buf) ?: return
                ctx.fireChannelRead(AddressedOfflinePacket(packet, sender))
            } catch (e: Exception) {
                log.warn("Failed to decode offline packet from {}: {}", sender, e.message, e)
            }
        }
    }
}
