package io.github.agent0876.raknetty.handler.event

import io.github.agent0876.raknetty.core.connection.RakNetConnection
import io.netty5.buffer.Buffer

/**
 * Application-layer message fired as [ChannelHandlerContext.fireChannelRead].
 *
 * [payload] is owned by the recipient and must be released when done.
 */
data class RakNetPacket(
    val connection: RakNetConnection,
    val payload: Buffer,
)
