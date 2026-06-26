package io.github.agent0876.raknetty.transport

import io.github.agent0876.raknetty.core.connection.DisconnectReason
import io.github.agent0876.raknetty.core.connection.RakNetConnection
import io.github.agent0876.raknetty.handler.event.RakNetEvent
import io.github.agent0876.raknetty.handler.event.RakNetPacket
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter

/**
 * Convenience base class for application-layer handlers.
 *
 * Override the typed callbacks instead of dealing with raw [channelRead] and
 * [userEventTriggered] dispatch. Unhandled messages and events are forwarded.
 *
 * **[onMessage] payload**: released automatically after [onMessage] returns —
 * call [ByteBuf.retain] inside [onMessage] if the buffer needs to outlive the call.
 */
abstract class SimpleRakNetHandler : ChannelInboundHandlerAdapter() {

    final override fun channelRead(ctx: ChannelHandlerContext, msg: Any) {
        when (msg) {
            is RakNetPacket -> {
                try {
                    onMessage(ctx, msg.connection, msg.payload)
                } finally {
                    msg.payload.release()
                }
            }
            else -> ctx.fireChannelRead(msg)
        }
    }

    final override fun userEventTriggered(ctx: ChannelHandlerContext, evt: Any) {
        when (evt) {
            is RakNetEvent.Connected    -> onConnect(ctx, evt.connection)
            is RakNetEvent.Disconnected -> onDisconnect(ctx, evt.connection, evt.reason)
            else -> ctx.fireUserEventTriggered(evt)
        }
    }

    /** A new RakNet connection has been fully established (post-handshake). */
    open fun onConnect(ctx: ChannelHandlerContext, connection: RakNetConnection) {}

    /**
     * An application-layer message has arrived on [connection].
     * [payload] is owned by this call; retain it if needed beyond the method scope.
     */
    open fun onMessage(ctx: ChannelHandlerContext, connection: RakNetConnection, payload: ByteBuf) {}

    /** [connection] has been closed with [reason]. */
    open fun onDisconnect(ctx: ChannelHandlerContext, connection: RakNetConnection, reason: DisconnectReason) {}

    override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
        ctx.fireExceptionCaught(cause)
    }
}
