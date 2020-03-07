package io.emeraldpay.polkadotcrawler.crawler

import io.emeraldpay.polkadotcrawler.polkadot.StatusProtocol
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import java.nio.ByteBuffer
import java.util.function.Function

/**
 * Request status of the blockchain
 */
class RequestStatusDetails: RequestDetails<StatusProtocol.Status>("/substrate/ksmcc3/6", CrawlerData.Type.STATUS) {

    companion object {
        private val log = LoggerFactory.getLogger(RequestStatusDetails::class.java)
        private val statusProtocol = StatusProtocol()
    }

    override fun process(): Function<Flux<ByteBuffer>, Flux<StatusProtocol.Status>> {
        return Function { flux ->
            flux.map(statusProtocol::parse)
        }
    }

}