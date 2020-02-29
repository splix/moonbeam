package io.emeraldpay.polkadotcrawler.processing

import io.emeraldpay.polkadotcrawler.state.ConnectionDetails
import io.emeraldpay.polkadotcrawler.state.PeerDetails
import io.emeraldpay.polkadotcrawler.state.ProcessedPeerDetails
import io.libp2p.core.multiformats.Protocol
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import java.time.Instant
import java.util.function.Function

@Service
class FullProcessor(): Function<PeerDetails, ProcessedPeerDetails> {

    companion object {
        private val log = LoggerFactory.getLogger(FullProcessor::class.java)
    }

    private val agentParser = AgentParser()

    override fun apply(peer: PeerDetails): ProcessedPeerDetails {
        val result = ProcessedPeerDetails()

        result.peerId = peer.peerId?.toString()
        //if peerid wasn't set by some reason try to extract from address
        if (result.peerId == null) {
            result.peerId = peer.address.getStringComponent(Protocol.P2P)
                    ?: peer.address.getStringComponent(Protocol.IPFS)
        }

        peer.agent?.let { agent ->
            result.agent = agentParser.parse(agent)
        }

        result.host = ProcessedPeerDetails.Host.from(peer)

        result.connection = ConnectionDetails(
                if (peer.incoming) { ConnectionDetails.ConnectionType.IN } else { ConnectionDetails.ConnectionType.OUT },
                peer.connectedAt,
                peer.disconnectedAt ?: Instant.now()
        )

        return result
    }

}