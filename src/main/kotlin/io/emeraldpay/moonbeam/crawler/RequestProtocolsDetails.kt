package io.emeraldpay.moonbeam.crawler

import io.emeraldpay.moonbeam.libp2p.Multistream
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import java.nio.ByteBuffer
import java.util.function.Function

/**
 * Request list of all supported Libp2p protocols
 */
class RequestProtocolsDetails: RequestDetails<CrawlerData.StringList>("ls", CrawlerData.Type.PROTOCOLS) {

    companion object {
        private val log = LoggerFactory.getLogger(RequestProtocolsDetails::class.java)
        private val multistream = Multistream()
    }

    override fun confirmProtocol(): String? {
        return null
    }

    override fun process(): Function<Flux<ByteBuffer>, Flux<CrawlerData.StringList>> {
        return Function { flux ->
            flux.map(multistream::parseList)
                .map(CrawlerData::StringList)
        }
    }

}