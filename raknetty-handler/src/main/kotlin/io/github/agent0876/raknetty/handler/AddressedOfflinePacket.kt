package io.github.agent0876.raknetty.handler

import io.github.agent0876.raknetty.codec.offline.OfflinePacket
import java.net.InetSocketAddress

/** Wrapper passed up the pipeline after [DatagramDispatcher] decodes an offline packet. */
data class AddressedOfflinePacket(
    val packet: OfflinePacket,
    val sender: InetSocketAddress,
)
