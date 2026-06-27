package io.github.moltenmc.raknetty.core.util

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class SequenceNumberTest {

    private val MAX = SequenceNumber.MAX

    @Test fun `isGreaterThan simple case`() {
        assertTrue(SequenceNumber.isGreaterThan(5, 3))
        assertFalse(SequenceNumber.isGreaterThan(3, 5))
        assertFalse(SequenceNumber.isGreaterThan(3, 3))
    }

    @Test fun `isGreaterThan wraps correctly`() {
        // MAX is "just before" 0 — 0 should be considered newer than MAX
        assertTrue(SequenceNumber.isGreaterThan(0, MAX))
        assertFalse(SequenceNumber.isGreaterThan(MAX, 0))
    }

    @Test fun `distance no wrap`() {
        assertEquals(3, SequenceNumber.distance(5, 2))
        assertEquals(0, SequenceNumber.distance(7, 7))
    }

    @Test fun `distance with wrap`() {
        // From MAX to 1 forward is 2
        assertEquals(2, SequenceNumber.distance(1, MAX - 1 + 1))
    }

    @Test fun `increment wraps at MAX`() {
        assertEquals(0, SequenceNumber.increment(MAX))
        assertEquals(1, SequenceNumber.increment(MAX, 2))
    }

    @Test fun `increment normal`() {
        assertEquals(10, SequenceNumber.increment(9))
        assertEquals(15, SequenceNumber.increment(10, 5))
    }
}
