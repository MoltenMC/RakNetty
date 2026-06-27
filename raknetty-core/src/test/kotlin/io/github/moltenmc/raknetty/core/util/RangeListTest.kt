package io.github.moltenmc.raknetty.core.util

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class RangeListTest {

    private fun rangeList(vararg pairs: Pair<Int, Int>): RangeList {
        val list = RangeList()
        for ((s, e) in pairs) list.addRange(s, e)
        return list
    }

    @Test fun `add single values creates individual ranges`() {
        val list = RangeList().also { it.add(3); it.add(7); it.add(10) }
        assertEquals(listOf(3..3, 7..7, 10..10), list.ranges)
    }

    @Test fun `adjacent values are merged`() {
        val list = RangeList().also { it.add(5); it.add(6); it.add(7) }
        assertEquals(listOf(5..7), list.ranges)
        assertEquals(3, list.size)
    }

    @Test fun `overlapping ranges merge`() {
        val list = rangeList(0 to 5, 3 to 10)
        assertEquals(listOf(0..10), list.ranges)
    }

    @Test fun `non-adjacent ranges stay separate`() {
        val list = rangeList(0 to 5, 7 to 10)
        assertEquals(listOf(0..5, 7..10), list.ranges)
    }

    @Test fun `adding range that spans multiple existing ranges collapses them`() {
        val list = rangeList(0 to 2, 5 to 7, 10 to 12)
        list.addRange(1, 11)
        assertEquals(listOf(0..12), list.ranges)
    }

    @Test fun `insert in the middle`() {
        val list = rangeList(0 to 5, 20 to 25)
        list.addRange(10, 15)
        assertEquals(listOf(0..5, 10..15, 20..25), list.ranges)
    }

    @Test fun `addRange prepend`() {
        val list = rangeList(10 to 20)
        list.addRange(0, 5)
        assertEquals(listOf(0..5, 10..20), list.ranges)
    }

    @Test fun `addRange append`() {
        val list = rangeList(0 to 5)
        list.addRange(10, 20)
        assertEquals(listOf(0..5, 10..20), list.ranges)
    }

    @Test fun `clear empties the list`() {
        val list = rangeList(0 to 100)
        list.clear()
        assertTrue(list.isEmpty())
        assertEquals(0, list.size)
    }

    @Test fun `size counts total integers`() {
        val list = rangeList(0 to 4, 10 to 14)
        assertEquals(10, list.size)
    }
}
