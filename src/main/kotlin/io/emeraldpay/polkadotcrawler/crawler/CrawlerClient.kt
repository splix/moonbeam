package io.emeraldpay.polkadotcrawler.crawler

import identify.pb.IdentifyOuterClass
import io.emeraldpay.polkadotcrawler.libp2p.*
import io.emeraldpay.polkadotcrawler.proto.Dht
import io.libp2p.core.crypto.PrivKey
import io.libp2p.core.crypto.PubKey
import io.libp2p.core.multiformats.Multiaddr
import io.libp2p.core.multiformats.Protocol
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.ConnectTimeoutException
import io.netty.handler.logging.LogLevel
import io.netty.handler.logging.LoggingHandler
import org.reactivestreams.Publisher
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.extra.processor.TopicProcessor
import reactor.netty.Connection
import reactor.netty.NettyInbound
import reactor.netty.NettyOutbound
import reactor.netty.tcp.TcpClient
import java.net.UnknownHostException
import java.nio.charset.Charset
import java.time.Duration

class CrawlerClient(
        private val remote: Multiaddr,
        private val agent: IdentifyOuterClass.Identify,
        private val keys: Pair<PrivKey, PubKey>
) {

    companion object {
        private val log = LoggerFactory.getLogger(CrawlerClient::class.java)
    }

    private val multistream = Multistream()

    private val results = TopicProcessor.builder<Data<*>>()
            .share(true)
            .bufferSize(8)
            .autoCancel(false)
            .build()

    fun getHost(): String {
        if (remote.has(Protocol.IP4)) {
            return remote.getStringComponent(Protocol.IP4)!!
        }
        if (remote.has(Protocol.DNS4)) {
            return remote.getStringComponent(Protocol.DNS4)!!
        }
        throw IllegalArgumentException("Unsupported address: $remote")
    }


    fun connect(): Flux<Data<*>> {
        log.debug("Connect to $remote")
        val host = getHost()
        val port = remote.getStringComponent(Protocol.TCP)!!.toInt()

        try {
            val connection: Connection = TcpClient.create()
                    .host(host)
                    .port(port)
                    .handle(::handle)
                    .connectNow(Duration.ofSeconds(15))

            connection.onDispose().subscribe {
                log.debug("Disconnected from $remote")
            }
            return Flux.from(results)
                    .doFinally {
                        connection.dispose()
                    }
        } catch (e: ConnectTimeoutException) {
            log.debug("Timeout to connect to $remote")
        } catch (e: IllegalStateException) {
            log.debug("Failed to connect to $remote. Message: ${e.message}")
        } catch (e: UnknownHostException) {
            log.warn("Invalid remote address: $remote")
        } catch (e: Throwable) {
            log.error("Unresolved exception ${e.javaClass.name}: ${e.message}")
        }

        return Flux.empty()
    }

    fun requestDht(mplex: Mplex): Mono<Data<Dht.Message>> {
        return mplex.newStream(object: Mplex.Handler<Mono<Data<Dht.Message>>> {
            override fun handle(id: Long, inboud: Publisher<ByteBuf>, outboud: Mplex.MplexOutbound): Mono<Data<Dht.Message>> {
                val dht = DhtProtocol()
                val result = Flux.from(inboud)
                        .transform(SizePrefixed.Varint().reader())
//                        .transform(DebugCommons.traceByteBuf("DHT PACKET"))
                        .transform(multistream.readProtocol("/ipfs/kad/1.0.0", false))
                        .filter {
                            it.readableBytes() > 2 //skip 0x1a00 TODO
                        }
                        .map {
                            dht.parse(it)
                        }
                        .take(1).single().timeout(Duration.ofSeconds(15), Mono.error(DataTimeoutException("DHT")))
                        .doOnError {
                            if (it is DataTimeoutException) {
                                log.debug("Timeout for ${it.name} from $remote")
                            } else {
                                log.warn("DHT not received", it)
                            }
                        }
                        .onErrorResume { Mono.empty() }
                        .map { Data(DataType.DHT_NODES, it) }
                val start = Mono.just(multistream.headerFor("/ipfs/kad/1.0.0"))
                return outboud.send(
                        Flux.concat(start, dht.start())
                ).then(result)
            }
        })
    }

    fun requestIdentify(mplex: Mplex): Mono<Data<IdentifyOuterClass.Identify>> {
        return mplex.newStream(object: Mplex.Handler<Mono<Data<IdentifyOuterClass.Identify>>> {
            override fun handle(id: Long, inboud: Publisher<ByteBuf>, outboud: Mplex.MplexOutbound): Mono<Data<IdentifyOuterClass.Identify>> {
                val identify = IdentifyProtocol()
                val result  = Flux.from(inboud)
                        .transform(SizePrefixed.Varint().reader())
//                        .transform(DebugCommons.traceByteBuf("ID PACKET"))
                        .transform(multistream.readProtocol("/ipfs/id/1.0.0", false))
                        .map {
                            identify.parse(it)
                        }
                        .take(1).single().timeout(Duration.ofSeconds(15), Mono.error(DataTimeoutException("Identify")))
                        .doOnError {
                            if (it is DataTimeoutException) {
                                log.debug("Timeout for ${it.name} from $remote")
                            } else {
                                log.warn("Identity not received", it)
                            }
                        }
                        .onErrorResume { Mono.empty() }
                        .map { Data(DataType.IDENTIFY, it) }
                val start = Mono.just(multistream.headerFor("/ipfs/id/1.0.0"))
                return outboud.send(start).then(result)
            }
        })
    }

    fun respondStandardRequests(mplex: Mplex): Mono<Void> {
        return mplex.receiveStreams(object: Mplex.Handler<Mono<Void>> {
            override fun handle(id: Long, inboud: Publisher<ByteBuf>, outboud: Mplex.MplexOutbound): Mono<Void> {
                val repl = Flux.from(inboud)
                        .switchOnFirst { signal, flux ->
                            if (signal.hasValue()) {
                                val msg = signal.get()!!
                                if (msg.toString(Charset.defaultCharset()).contains("/ipfs/ping/1.0.0")) {
                                    val start = Mono.just(multistream.headerFor("/ipfs/ping/1.0.0"))
                                    return@switchOnFirst Flux.concat(start, flux.skip(1))
                                } else if (msg.toString(Charset.defaultCharset()).contains("/ipfs/id/1.0.0")) {
                                    val value = Mono.just(Unpooled.wrappedBuffer(agent.toByteArray()))
                                    return@switchOnFirst Flux.concat(value)
                                } else {
                                    log.debug("Request to an unsupported protocol")
                                }
                            }
                            Flux.empty<ByteBuf>()
                        }
                return outboud.send(repl)
            }
        }).flatMap { it }.then()
    }

    fun handle(inbound: NettyInbound, outbound: NettyOutbound): Publisher<Void> {
//        outbound.withConnection { conn ->
//            conn.addHandler(LoggingHandler("io.netty.util.internal.logging.Slf4JLogger", LogLevel.INFO))
//        }

        val secio = Secio(keys.first)
        val mplex = Mplex()

        val proposeSecio = Mono.just(multistream.write(
                Unpooled.wrappedBuffer(secio.propose().toByteArray()),
                "/secio/1.0.0"
        ))

        val receive = inbound.receive().share()

        val establishSecio = secio.readSecio(receive)

        val decrypted =  receive
                .skipUntil { secio.isEstablished() }
                .transform(SizePrefixed.Standard().reader())
                .transform(secio.frameDecoder())
                .share()

        secio.readNonce(decrypted).subscribe()

        decrypted
                .transform(multistream.readProtocol("/mplex/6.7.0"))
                .map(mplex::onNext)
                .doOnError { log.warn("Failed to process Mplex message", it) }
                .subscribe()

        val mplexSender = Flux.from(mplex.start())
                .map(secio.encoder())
                .doOnError { log.error("Failed to start Mplex", it) }

        val mplexResponse = respondStandardRequests(mplex)

        val processors: Publisher<Data<*>> = Flux.merge(
                requestIdentify(mplex),
                requestDht(mplex)
        )

        mplexResponse.subscribe()
        processors.subscribe(results)

        val main: Publisher<Void> = outbound.send(Flux.concat(proposeSecio, establishSecio, mplexSender))


        return main
    }


    //
    // ------------------------
    //

    enum class DataType(val clazz: Class<out Any>) {
        IDENTIFY(IdentifyOuterClass.Identify::class.java),
        DHT_NODES(Dht.Message::class.java),
        FINALIZED(FinalizedType::class.java)
    }

    enum class FinalizedType {
        SENDER,
        MPLEX_RESPONSE
    }

    class Data<T>(
            val dataType: DataType,
            val data: T
    ) {
        fun <Z> cast(clazz: Class<Z>): Data<Z> {
            if (!clazz.isAssignableFrom(this.dataType.clazz)) {
                throw ClassCastException("Cannot cast ${this.dataType.clazz} to $clazz")
            }
            return this as Data<Z>;
        }
    }

    class DataTimeoutException(val name: String): java.lang.Exception("Timeout for $name")

}