package io.emeraldpay.moonbeam.state

import org.slf4j.LoggerFactory
import java.time.Instant

class ConnectionDetails (
        val connectionType: ConnectionType,
        val connectedAt: Instant,
        val disconnectedAt: Instant
) {

    companion object {
        private val log = LoggerFactory.getLogger(ConnectionDetails::class.java)
    }

    enum class ConnectionType {
        IN, OUT
    }

}