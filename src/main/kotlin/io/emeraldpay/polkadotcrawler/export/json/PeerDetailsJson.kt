package io.emeraldpay.polkadotcrawler.export.json

import com.fasterxml.jackson.annotation.JsonPropertyOrder
import io.emeraldpay.polkadotcrawler.state.PeerDetails
import io.emeraldpay.polkadotcrawler.state.ProcessedPeerDetails
import io.libp2p.core.multiformats.Protocol
import org.slf4j.LoggerFactory
import java.net.InetAddress
import java.time.Instant

/**
 * JSON representation of Peer Details
 */
@JsonPropertyOrder(value = ["version", "timestamp"])
class PeerDetailsJson(timestamp: Instant): ProcessedPeerDetails(timestamp) {

    companion object {
        private val log = LoggerFactory.getLogger(PeerDetailsJson::class.java)

        fun from(peer: ProcessedPeerDetails): PeerDetailsJson {
            val result = PeerDetailsJson(peer.timestamp)
            result.agent = peer.agent
            result.host = peer.host
            return result
        }
    }

    val version: String = "https://schema.emeraldpay.io/polkadbot#v0"

}