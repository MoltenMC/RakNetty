package io.github.moltenmc.raknetty.core.util

import io.netty5.buffer.Buffer

/**
 * A sorted, merged list of inclusive integer ranges.
 *
 * Used for ACK/NAK batching: instead of sending one record per sequence number,
 * consecutive numbers are collapsed into `[start..end]` ranges.
 *
 * All operations are O(n) where n is the current number of disjoint ranges,
 * which in practice stays very small (typical window is 512–2048 datagrams).
 */
class RangeList {

    private val _ranges = mutableListOf<IntRange>()

    val ranges: List<IntRange> get() = _ranges
    fun isEmpty(): Boolean = _ranges.isEmpty()

    /** Total count of individual integers across all ranges. */
    val size: Int get() = _ranges.sumOf { it.last - it.first + 1 }

    fun add(n: Int) = addRange(n, n)

    fun addRange(start: Int, end: Int) {
        require(start <= end)
        var lo = start
        var hi = end
        var insertPos = _ranges.size

        var i = 0
        while (i < _ranges.size) {
            val r = _ranges[i]
            when {
                r.last < lo - 1 -> {
                    // r ends before [lo..hi] with a gap: skip
                    insertPos = i + 1
                    i++
                }
                r.first > hi + 1 -> {
                    // r starts after [lo..hi] with a gap: insert before it
                    insertPos = i
                    break
                }
                else -> {
                    // overlapping or adjacent: absorb r into [lo..hi]
                    lo = minOf(lo, r.first)
                    hi = maxOf(hi, r.last)
                    _ranges.removeAt(i)
                    insertPos = i
                    // i now points to the next element after removal
                }
            }
        }

        _ranges.add(insertPos, lo..hi)
    }

    fun clear() = _ranges.clear()

    // ── Wire encoding (RakNet ACK/NAK datagram body) ─────────────────────────

    fun encode(buf: Buffer) {
        buf.writeShort(_ranges.size.toShort())
        for (r in _ranges) {
            val single = r.first == r.last
            buf.writeBoolean(single)
            buf.writeMediumLE(r.first)
            if (!single) buf.writeMediumLE(r.last)
        }
    }

    companion object {
        fun decode(buf: Buffer): RangeList {
            val list = RangeList()
            val count = buf.readUnsignedShort()
            repeat(count) {
                val single = buf.readBoolean()
                val start  = buf.readUnsignedMediumLE()
                val end    = if (single) start else buf.readUnsignedMediumLE()
                list.addRange(start, end)
            }
            return list
        }
    }
}
