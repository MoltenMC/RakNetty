package io.github.agent0876.raknetty.transport

import io.github.agent0876.raknetty.core.connection.DisconnectReason
import io.github.agent0876.raknetty.core.connection.RakNetConnection
import io.github.agent0876.raknetty.handler.event.RakNetEvent
import io.github.agent0876.raknetty.handler.event.RakNetPacket
import io.netty5.buffer.Buffer
import io.netty5.channel.ChannelHandlerContext
import io.netty5.channel.SimpleChannelInboundHandler

abstract class SimpleRakNetHandler : SimpleChannelInboundHandler<RakNetPacket>() {

    final override fun messageReceived(ctx: ChannelHandlerContext, msg: RakNetPacket) {
        try {
            onMessage(ctx, msg.connection, msg.payload)
        } finally {
            msg.payload.close()
        }
    }

    final override fun channelInboundEvent(ctx: ChannelHandlerContext, evt: Any) {
        when (evt) {
            is RakNetEvent.Connected    -> onConnect(ctx, evt.connection)
            is RakNetEvent.Disconnected -> onDisconnect(ctx, evt.connection, evt.reason)
            else -> ctx.fireChannelInboundEvent(evt)
        }
    }

    open fun onConnect(ctx: ChannelHandlerContext, connection: RakNetConnection) {}

    open fun onMessage(ctx: ChannelHandlerContext, connection: RakNetConnection, payload: Buffer) {}

    open fun onDisconnect(ctx: ChannelHandlerContext, connection: RakNetConnection, reason: DisconnectReason) {}
}
