package io.github.agent0876.raknetty.handler.reliability

import io.github.agent0876.raknetty.core.packet.RakNetFrame
import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufAllocator
import java.util.HashMap

/**
 * Reassembles split (fragmented) RakNet messages.
 *
 * Fragments sharing the same [SplitInfo.splitId] are collected until all
 * [SplitInfo.splitCount] pieces arrive, then merged into a single composite buffer.
 * Duplicate fragments are silently dropped.
 *
 * Incomplete sets are evicted by [removeExpired] after [timeoutMs] milliseconds
 * to prevent memory leaks when a sender disappears mid-transfer.
 */
class FragmentAccumulator {

    private data class FragmentSet(
        val splitId: Int,
        val splitCount: Int,
        val createdAt: Long,
        val fragments: Array<ByteBuf?> = arrayOfNulls(splitCount),
        var received: Int = 0,
    )

    private val sets = HashMap<Int, FragmentSet>() // splitId → in-progress set

    /**
     * Adds [frame]'s fragment to the accumulator.
     *
     * Returns the fully reassembled payload when the last fragment arrives,
     * or `null` when more fragments are still expected. The returned [ByteBuf] is
     * a composite owned by the caller — caller must release it.
     *
     * [frame.payload] is copied internally; callers must still release [frame.payload]
     * themselves after this call returns.
     */
    fun accumulate(frame: RakNetFrame, alloc: ByteBufAllocator): ByteBuf? {
        val split = requireNotNull(frame.split) { "Frame must have SplitInfo" }
        val set = sets.getOrPut(split.splitId) {
            FragmentSet(split.splitId, split.splitCount, System.currentTimeMillis())
        }

        if (set.fragments[split.splitIndex] != null) return null // duplicate fragment

        set.fragments[split.splitIndex] = frame.payload.copy()
        set.received++

        if (set.received < set.splitCount) return null

        // All fragments received — reassemble zero-copy via composite buffer
        sets.remove(split.splitId)
        val composite = alloc.compositeBuffer(set.splitCount)
        for (i in 0 until set.splitCount) {
            composite.addComponent(true, requireNotNull(set.fragments[i]))
        }
        return composite
    }

    /**
     * Releases and removes any fragment sets that have been waiting longer than
     * [timeoutMs]. Call once per tick to bound memory usage.
     *
     * @return the number of sets that were evicted
     */
    fun removeExpired(now: Long, timeoutMs: Long): Int {
        val expired = sets.values.filter { now - it.createdAt >= timeoutMs }
        for (set in expired) {
            set.fragments.forEach { it?.release() }
            sets.remove(set.splitId)
        }
        return expired.size
    }

    /** Releases all partially accumulated fragments. Call when the connection closes. */
    fun release() {
        sets.values.forEach { set ->
            set.fragments.forEach { it?.release() }
        }
        sets.clear()
    }
}
