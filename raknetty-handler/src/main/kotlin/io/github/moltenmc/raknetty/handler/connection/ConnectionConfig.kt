package io.github.moltenmc.raknetty.handler.connection

/**
 * Tunable reliability and lifecycle parameters for a single [RakNetConnectionImpl].
 * All time values are in milliseconds.
 */
data class ConnectionConfig(
    /** Initial RTO before any RTT sample has been collected. */
    val initialRto: Long = 200L,
    /** Minimum RTO — prevents the retransmit timer from collapsing on fast links. */
    val minRto: Long = 100L,
    /** Maximum RTO — caps exponential backoff under severe packet loss. */
    val maxRto: Long = 3_000L,
    /** Starting congestion window in datagrams (per RFC 5681 § 3.1). */
    val initialCwnd: Int = 4,
    /** Interval between keepalive ConnectedPing packets when the connection is idle. */
    val pingInterval: Long = 5_000L,
    /** Inactivity duration after which the connection is considered timed out. */
    val connectionTimeout: Long = 10_000L,
    /** Hard cap on the congestion window — also used as initial ssthresh. */
    val maxUnackedDatagrams: Int = 512,
    /** Bytes subtracted from MTU to compute the fragment threshold (header overhead). */
    val fragmentOverhead: Int = 60,
    /** Tick interval for ACK/NAK flush and retransmit checks. */
    val tickIntervalMs: Long = 10L,
    /** How long a partially-received split packet may linger before its fragments are released. */
    val fragmentTimeout: Long = 30_000L,
    /** How long to wait for OCReply1 before retrying OCReq1 with the next (smaller) MTU probe. */
    val ocReq1RetryIntervalMs: Long = 1_000L,
)
