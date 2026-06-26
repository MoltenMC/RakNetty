package io.github.agent0876.raknetty.core.protocol

object RakNetProtocol {
    /** Protocol version sent in OCReq1/2 and checked in OCReply1/2. Minecraft-compatible. */
    const val VERSION: Int = 11

    /** Maximum ordering channels per connection (hard limit from the spec). */
    const val MAX_ORDER_CHANNELS: Int = 32

    /** Maximum number of fragments a single message can be split into. */
    const val MAX_SPLIT_COUNT: Int = 128

    /**
     * MTU sizes probed during handshake, in descending order.
     * OCReq1 is sent with each size padded to the full MTU until one succeeds.
     */
    val MTU_SIZES: IntArray = intArrayOf(1492, 1200, 576)
}
