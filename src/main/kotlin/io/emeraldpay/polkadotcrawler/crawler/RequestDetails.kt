package io.emeraldpay.polkadotcrawler.crawler

import io.emeraldpay.polkadotcrawler.libp2p.Mplex
import io.emeraldpay.polkadotcrawler.libp2p.Multistream
import io.emeraldpay.polkadotcrawler.libp2p.SizePrefixed
import org.reactivestreams.Publisher
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.util.function.Tuple2
import reactor.util.function.Tuples
import java.io.IOException
import java.nio.ByteBuffer
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.function.Function

/**
 * Base class to request details about remote peer
 */
abstract class RequestDetails<T>(
        private val protocol: String,
        private val type: CrawlerData.Type
) {

    companion object {
        private val log = LoggerFactory.getLogger(RequestDetails::class.java)
        private val MULTISTREAM = Multistream()
        private val SIZE_SPLIT = SizePrefixed.Varint().reader()
    }

    abstract fun process(): Function<Flux<ByteBuffer>, Flux<T>>

    /**
     * Send the initial message(s) to request details from remote
     */
    open fun start(): Publisher<ByteBuffer> {
        return Mono.empty()
    }

    /**
     * Protocol name to propose
     */
    open fun proposeProtocol(): String {
        return protocol
    }

    /**
     * Protocol name expected from remote, by default is same as .proposeProtocol()
     */
    open fun confirmProtocol(): String? {
        return proposeProtocol()
    }

    /**
     * Request that expects only one returning item
     */
    fun requestOne(mplex: Mplex): Tuple2<
            Mono<CrawlerData.Value<T>>,
            Publisher<ByteBuffer>> {
        val result = request(mplex)
        return Tuples.of(result.t1.next(), result.t2)
    }

    fun request(mplex: Mplex): Tuple2<
            Flux<CrawlerData.Value<T>>,
            Publisher<ByteBuffer>> {

        val stream = mplex.newStream(Mono.just(MULTISTREAM.multistreamHeader(proposeProtocol())))

        // wait for header received, then .start() requests
        val headerReceived = CompletableFuture<Boolean>()
        stream.send(Mono.fromCompletionStage(headerReceived).thenMany(start()))
        val onHeaderFound = Mono.just(headerReceived).doOnNext { it.complete(true) }.then()

        val result = Flux.from(stream.inbound)
                .transform(SIZE_SPLIT)
                //.transform(DebugCommons.traceByteBuf("Request data $protocol", false))
                .transform(MULTISTREAM.readProtocol(confirmProtocol(), false, onHeaderFound))
                .take(Duration.ofSeconds(15))
                .timeout(Duration.ofSeconds(10), Mono.error(CrawlerClient.DataTimeoutException(protocol)))
                .transform(process())
                .doOnError { t ->
                    when (t) {
                        is CrawlerClient.DataTimeoutException -> {
                            log.debug("Timeout for ${t.name} from remote")
                        }
                        is IOException -> {
                            log.debug("$protocol not received. ${t.message}")
                        }
                        else -> {
                            log.warn("$protocol not received", t)
                        }
                    }
                }
                .onErrorResume { Mono.empty() }
                .map { CrawlerData.Value(type, it) }

        return Tuples.of(result, stream.outbound())
    }

}