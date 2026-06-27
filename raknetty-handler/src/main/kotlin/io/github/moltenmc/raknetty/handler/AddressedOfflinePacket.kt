package io.github.moltenmc.raknetty.handler

import io.github.moltenmc.raknetty.codec.offline.OfflinePacket
import java.net.InetSocketAddress

/** Wrapper passed up the pipeline after [DatagramDispatcher] decodes an offline packet. */
data class AddressedOfflinePacket(
    val packet: OfflinePacket,
    val sender: InetSocketAddress,
)
