package io.emeraldpay.moonbeam.crawler

import identify.pb.IdentifyOuterClass
import io.emeraldpay.moonbeam.libp2p.IdentifyProtocol
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import java.nio.ByteBuffer
import java.util.function.Function

/**
 * Request remote Identify data
 */
class RequestIdDetails:
        RequestDetails<IdentifyOuterClass.Identify>("/ipfs/id/1.0.0", CrawlerData.Type.IDENTIFY) {

    companion object {
        private val log = LoggerFactory.getLogger(RequestIdDetails::class.java)
        private val identify = IdentifyProtocol()
    }

    override fun process(): Function<Flux<ByteBuffer>, Flux<IdentifyOuterClass.Identify>> {
        return Function { flux ->
            flux.map(identify::parse)
        }
    }

}