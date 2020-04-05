package io.emeraldpay.moonbeam.export.json

import com.fasterxml.jackson.annotation.JsonPropertyOrder
import io.emeraldpay.moonbeam.state.*
import io.libp2p.core.multiformats.Multiaddr
import io.libp2p.core.multiformats.Protocol
import org.slf4j.LoggerFactory
import java.net.InetAddress
import java.time.Instant

/**
 * JSON representation of Peer Details
 */
@JsonPropertyOrder(value = ["version", "timestamp"])
class PeerDetailsJson(val timestamp: Instant) {

    companion object {
        private val log = LoggerFactory.getLogger(PeerDetailsJson::class.java)

        fun from(peer: ProcessedPeerDetails): PeerDetailsJson {
            val result = PeerDetailsJson(peer.timestamp)
            result.peerId = peer.peerId
            result.agent = peer.agent
            result.host = peer.host
            result.connection = peer.connection
            result.blockchain = peer.blockchain
            result.protocols = peer.protocols
            return result
        }
    }

    val version: String = "https://schema.emeraldpay.io/moonbeam"

    var peerId: String? = null
    var agent: Agent? = null
    var host: ProcessedPeerDetails.Host? = null
    var connection: ConnectionDetails? = null
    var blockchain: Blockchain? = null
    var protocols: Protocols? = null
}