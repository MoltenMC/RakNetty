package io.github.moltenmc.raknetty.handler.event

import io.github.moltenmc.raknetty.core.connection.DisconnectReason
import io.github.moltenmc.raknetty.core.connection.RakNetConnection

/** User events fired via [ChannelHandlerContext.fireChannelInboundEvent]. */
sealed class RakNetEvent {
    data class Connected(val connection: RakNetConnection) : RakNetEvent()
    data class Disconnected(val connection: RakNetConnection, val reason: DisconnectReason) : RakNetEvent()
}
