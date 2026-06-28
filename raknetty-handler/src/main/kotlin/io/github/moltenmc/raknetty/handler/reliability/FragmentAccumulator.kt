package io.github.moltenmc.raknetty.handler.reliability

import io.github.moltenmc.raknetty.core.packet.RakNetFrame
import io.github.moltenmc.raknetty.core.protocol.RakNetProtocol
import io.netty5.buffer.Buffer
import io.netty5.buffer.BufferAllocator

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
        val fragments: Array<Buffer?> = arrayOfNulls(splitCount),
        var received: Int = 0,
    )

    private val sets = HashMap<Int, FragmentSet>() // splitId → in-progress set

    /**
     * Adds [frame]'s fragment to the accumulator.
     *
     * Returns the fully reassembled payload when the last fragment arrives,
     * or `null` when more fragments are still expected. The returned [Buffer] is
     * a composite owned by the caller — caller must release it.
     *
     * [frame.payload] is retained internally; callers must still release [frame.payload]
     * themselves after this call returns.
     */
    fun accumulate(frame: RakNetFrame, alloc: BufferAllocator, now: Long = System.currentTimeMillis()): Buffer? {
        val split = requireNotNull(frame.split) { "Frame must have SplitInfo" }
        if (split.splitCount !in 1..RakNetProtocol.MAX_SPLIT_COUNT) {
            frame.payload.close()
            return null
        }
        val set = sets.getOrPut(split.splitId) {
            FragmentSet(split.splitId, split.splitCount, now)
        }

        if (split.splitCount != set.splitCount) {
            frame.payload.close()
            return null          // inconsistent set — drop
        }
        if (split.splitIndex !in 0 until set.splitCount) {
            frame.payload.close()
            return null // out-of-range index — drop
        }
        if (set.fragments[split.splitIndex] != null) {
            frame.payload.close()     // duplicate fragment
            return null
        }

        set.fragments[split.splitIndex] = frame.payload
        set.received++

        if (set.received < set.splitCount) return null

        // All fragments received — reassemble via composite buffer
        sets.remove(split.splitId)
        val composite = alloc.compose()
        try {
            for (i in 0 until set.splitCount) {
                composite.extendWith(requireNotNull(set.fragments[i]).send())
            }
        } catch (t: Throwable) {
            composite.close()
            throw t
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
            set.fragments.forEach { it?.close() }
            sets.remove(set.splitId)
        }
        return expired.size
    }

    /** Releases all partially accumulated fragments. Call when the connection closes. */
    fun release() {
        sets.values.forEach { set ->
            set.fragments.forEach { it?.close() }
        }
        sets.clear()
    }
}
