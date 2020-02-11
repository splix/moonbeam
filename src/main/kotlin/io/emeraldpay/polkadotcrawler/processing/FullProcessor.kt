package io.emeraldpay.polkadotcrawler.processing

import io.emeraldpay.polkadotcrawler.state.PeerDetails
import io.emeraldpay.polkadotcrawler.state.ProcessedPeerDetails
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import java.util.function.Function

@Service
class FullProcessor(): Function<PeerDetails, ProcessedPeerDetails> {

    companion object {
        private val log = LoggerFactory.getLogger(FullProcessor::class.java)
    }

    private val agentParser = AgentParser()

    override fun apply(peer: PeerDetails): ProcessedPeerDetails {
        val result = ProcessedPeerDetails()

        peer.agent?.let { agent ->
            result.agent = agentParser.parse(agent)
        }

        result.host = ProcessedPeerDetails.Host.from(peer)

        return result
    }

}