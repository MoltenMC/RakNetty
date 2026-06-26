package io.github.agent0876.raknetty.handler.event

import io.github.agent0876.raknetty.core.connection.DisconnectReason
import io.github.agent0876.raknetty.core.connection.RakNetConnection

/** User events fired via [ChannelHandlerContext.fireChannelInboundEvent]. */
sealed class RakNetEvent {
    data class Connected(val connection: RakNetConnection) : RakNetEvent()
    data class Disconnected(val connection: RakNetConnection, val reason: DisconnectReason) : RakNetEvent()
}
