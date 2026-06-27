package io.github.moltenmc.raknetty.core.protocol

/**
 * Send priority for a RakNet message.
 *
 * [IMMEDIATE] packets bypass the send queue and are encoded and written to the
 * channel in the same call to [send()], without waiting for the next tick and
 * without being subject to the congestion-window limit. Use for latency-critical
 * or protocol-control messages (keepalive pings, disconnect notifications).
 *
 * [NORMAL] packets enter the regular send queue and are flushed in tick order,
 * respecting the AIMD congestion window. This is the correct choice for
 * application-layer data where throughput matters more than individual latency.
 */
enum class RakNetPriority {
    IMMEDIATE,
    NORMAL,
}
