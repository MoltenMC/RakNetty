package io.github.agent0876.raknetty.core.util

/**
 * Arithmetic helpers for RakNet's 24-bit rolling sequence numbers.
 *
 * Sequence numbers wrap at 2^24 (16_777_216). Comparisons use half-window
 * logic: a number is "greater" if the forward distance is less than 2^23.
 */
object SequenceNumber {

    const val BITS: Int  = 24
    const val MAX: Int   = (1 shl BITS) - 1   // 16_777_215
    private const val HALF: Int = 1 shl (BITS - 1)

    /** Returns `true` if [a] is strictly more recent than [b] (wrap-aware). */
    fun isGreaterThan(a: Int, b: Int): Boolean {
        val diff = (a - b) and MAX
        return diff in 1 until HALF
    }

    /** Forward distance from [older] to [newer] modulo 2^24. */
    fun distance(newer: Int, older: Int): Int = (newer - older) and MAX

    /** Increments [n] by [delta] with 24-bit wrap-around. */
    fun increment(n: Int, delta: Int = 1): Int = (n + delta) and MAX
}
