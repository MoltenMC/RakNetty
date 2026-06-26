package io.github.agent0876.raknetty.core.protocol

/**
 * Delivery guarantees for a single [RakNetFrame].
 *
 * The 3-bit reliability field in the frame flags maps to these values (id 0–7).
 * Ordered and sequenced both use an ordering channel (0–31); the distinction is
 * that *sequenced* packets that arrive out-of-order are dropped, while *ordered*
 * packets are held in a reorder buffer until the gap is filled.
 */
enum class Reliability(
    val id: Int,
    val isReliable: Boolean,
    val isOrdered: Boolean,
    val isSequenced: Boolean,
    val hasAckReceipt: Boolean = false,
) {
    UNRELIABLE                          (0, isReliable = false, isOrdered = false, isSequenced = false),
    UNRELIABLE_SEQUENCED                (1, isReliable = false, isOrdered = false, isSequenced = true),
    RELIABLE                            (2, isReliable = true,  isOrdered = false, isSequenced = false),
    RELIABLE_ORDERED                    (3, isReliable = true,  isOrdered = true,  isSequenced = false),
    RELIABLE_SEQUENCED                  (4, isReliable = true,  isOrdered = false, isSequenced = true),
    UNRELIABLE_WITH_ACK_RECEIPT         (5, isReliable = false, isOrdered = false, isSequenced = false, hasAckReceipt = true),
    RELIABLE_WITH_ACK_RECEIPT           (6, isReliable = true,  isOrdered = false, isSequenced = false, hasAckReceipt = true),
    RELIABLE_ORDERED_WITH_ACK_RECEIPT   (7, isReliable = true,  isOrdered = true,  isSequenced = false, hasAckReceipt = true);

    companion object {
        private val BY_ID = entries.associateBy { it.id }

        fun fromId(id: Int): Reliability =
            BY_ID[id] ?: throw IllegalArgumentException("Unknown reliability id: $id")
    }
}
