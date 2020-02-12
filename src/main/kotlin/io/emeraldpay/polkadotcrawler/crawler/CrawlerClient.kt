package io.emeraldpay.polkadotcrawler.crawler

import identify.pb.IdentifyOuterClass
import io.emeraldpay.polkadotcrawler.ByteBufferCommons
import io.emeraldpay.polkadotcrawler.libp2p.*
import io.emeraldpay.polkadotcrawler.proto.Dht
import io.libp2p.core.PeerId
import io.libp2p.core.crypto.PrivKey
import io.libp2p.core.crypto.PubKey
import io.libp2p.core.multiformats.Multiaddr
import io.libp2p.core.multiformats.Protocol
import io.netty.buffer.Unpooled
import io.netty.channel.ConnectTimeoutException
import io.netty.channel.unix.Errors
import io.netty.handler.logging.LogLevel
import io.netty.handler.logging.LoggingHandler
import org.reactivestreams.Publisher
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import reactor.extra.processor.TopicProcessor
import reactor.netty.Connection
import reactor.netty.NettyInbound
import reactor.netty.NettyOutbound
import reactor.netty.tcp.TcpClient
import reactor.util.function.Tuple2
import reactor.util.function.Tuples
import java.io.IOException
import java.net.UnknownHostException
import java.nio.ByteBuffer
import java.time.Duration
import java.util.concurrent.CancellationException
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicReference

class CrawlerClient(
        private val remote: Multiaddr,
        private val agent: IdentifyOuterClass.Identify,
        private val keys: Pair<PrivKey, PubKey>
) {

    companion object {
        private val log = LoggerFactory.getLogger(CrawlerClient::class.java)
    }

    private val multistream = Multistream()

    private var connection: Connection? = null


    fun getHost(): String {
        if (remote.has(Protocol.IP4)) {
            return remote.getStringComponent(Protocol.IP4)!!
        }
        if (remote.has(Protocol.DNS4)) {
            return remote.getStringComponent(Protocol.DNS4)!!
        }
        throw IllegalArgumentException("Unsupported address: $remote")
    }

    fun disconnect() {
        log.debug("Disconnecting from $remote")
        connection?.dispose()
    }

    fun connect(): Flux<Data<*>> {
        log.debug("Connect to $remote")
        val host = getHost()
        val port = remote.getStringComponent(Protocol.TCP)!!.toInt()

        try {
            val results = CompletableFuture<Flux<Data<*>>>()
            val client = TcpClient.create()
                    .host(host)
                    .port(port)
                    .handle { inbound, outbound ->
                        val x = handle(inbound, outbound)
                        results.complete(Flux.from(x.t2))
                        Flux.from(x.t1).onErrorResume { t ->
                            if (t is IOException) {
                                log.debug("Failed to connect to $remote")
                            } else {
                                log.warn("Handler failed to start", t)
                            }
                            if (!results.isDone) {
                                results.completeExceptionally(t)
                            }
                            Flux.empty()
                        }
                    }
                    .connect()
                    .doOnNext {
                        this.connection = it
                        it.onDispose().subscribe {
                            log.debug("Disconnected from $remote")
                        }
                    }

            val data = Mono.fromCompletionStage(results)
                    .flatMapMany { it }
                    .onErrorResume(CancellationException::class.java) { Flux.empty<Data<*>>() }
                    .doFinally {
                        connection?.dispose()
                    }

            return client.thenMany(data)
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

    fun requestDht(mplex: Mplex): Tuple2<
            Flux<Data<Dht.Message>>,
            Publisher<ByteBuffer>> {
        val dht = DhtProtocol()
        val stream = mplex.newStream(
                Mono.just(multistream.headerFor("/ipfs/kad/1.0.0"))
        )

        val headerReceived = CompletableFuture<Boolean>()

        stream.send(
                Mono.fromCompletionStage(headerReceived).thenMany(dht.start())
        )

        val onHeaderFound = Mono.just(headerReceived).doOnNext { it.complete(true) }.then()

        val result = Flux.from(stream.inbound)
                .transform(SizePrefixed.Varint().reader())
//                        .transform(DebugCommons.traceByteBuf("DHT PACKET"))
                .transform(
                        multistream.readProtocol("/ipfs/kad/1.0.0", false, onHeaderFound)
                )
                .filter { it.remaining() > 5 } //skip empty responses
                .take(3) //usually it returns no more than 3 messages
                .take(Duration.ofSeconds(15))
                .timeout(Duration.ofSeconds(10), Mono.error(DataTimeoutException("DHT")))
                .retry(3)
                .map {
                    dht.parse(it)
                }
                .doOnError {
                    if (it is DataTimeoutException) {
                        log.debug("Timeout for ${it.name} from $remote")
                    } else if (it is IOException) {
                        log.debug("DHT not received. ${it.message}")
                    } else {
                        log.warn("DHT not received", it)
                    }
                }
                .onErrorResume { Mono.empty() }
                .map { Data(DataType.DHT_NODES, it) }

        return Tuples.of(
                result,
                stream.outbound()
        )
    }

    fun requestIdentify(mplex: Mplex): Tuple2<
            Mono<Data<IdentifyOuterClass.Identify>>,
            Publisher<ByteBuffer>> {
        val identify = IdentifyProtocol()

        val stream = mplex.newStream(
                Mono.just(multistream.headerFor("/ipfs/id/1.0.0"))
        )
        val inbound: Publisher<ByteBuffer> = stream.inbound

        val result  = Flux.from(inbound)
                .transform(SizePrefixed.Varint().reader())
//                        .transform(DebugCommons.traceByteBuf("ID PACKET"))
                .transform(multistream.readProtocol("/ipfs/id/1.0.0", false))
                .map {
                    identify.parse(it)
                }
                .take(1).single()
                .timeout(Duration.ofSeconds(10), Mono.error(DataTimeoutException("Identify")))
                .retry(3)
                .doOnError {
                    if (it is DataTimeoutException) {
                        log.debug("Timeout for ${it.name} from $remote")
                    } else if (it is IOException) {
                        log.debug("Identify not received. ${it.message}")
                    } else {
                        log.warn("Identity not received", it)
                    }
                }
                .onErrorResume { Mono.empty() }
                .map { Data(DataType.IDENTIFY, it) }

        return Tuples.of(result, stream.outbound())
    }

    fun respondStandardRequests(mplex: Mplex): Flux<ByteBuffer> {
        return mplex.receiveStreams(object: Mplex.Handler<Flux<ByteBuffer>> {
            override fun handle(stream: Mplex.MplexStream): Flux<ByteBuffer> {
                val repl = Flux.from(stream.inbound)
                        .switchOnFirst { signal, flux ->
                            if (signal.hasValue()) {
                                val msg = signal.get()!!
                                if (ByteBufferCommons.contains(msg, "/ipfs/ping/1.0.0".toByteArray())) {
                                    val start = Mono.just(multistream.headerFor("/ipfs/ping/1.0.0"))
                                    val response = flux.skip(1).take(1).single()
                                    return@switchOnFirst Flux.concat(start, response)
                                } else if (ByteBufferCommons.contains(msg,"/ipfs/id/1.0.0".toByteArray())) {
                                    val value = ByteBuffer.wrap(agent.toByteArray())
                                    return@switchOnFirst Flux.just(value)
                                } else {
                                    log.debug("Request to an unsupported protocol")
                                }
                            }
                            Flux.empty<ByteBuffer>()
                        }
                return stream.send(repl).outbound()
            }
        }).flatMap { it }
    }

    fun handle(inbound: NettyInbound, outbound: NettyOutbound): Tuple2<Publisher<Void>, Publisher<Data<*>>> {
//        outbound.withConnection { conn ->
//            conn.addHandler(LoggingHandler("io.netty.util.internal.logging.Slf4JLogger", LogLevel.INFO))
//        }

        val secio = Secio(keys.first)
        val mplex = Mplex()

        val proposeSecio = Flux.concat(
                Mono.just(multistream.headerFor("/secio/1.0.0")),
                Mono.just(SizePrefixed.Standard().write(ByteBuffer.wrap(secio.propose().toByteArray())))
        )

        val receive = inbound.receive()
                .map {
                    val copy = ByteBuffer.allocate(it.readableBytes())
                    it.readBytes(copy)
//                    it.release()
                    copy.flip()
                }
                .share()


        val establishSecio = secio.readSecio(receive)

        val decrypted =  receive
                .skipUntil { secio.isEstablished() }
                .transform(SizePrefixed.Standard().reader())
                .transform(secio.frameDecoder())
                .share()

        val receiveSecioNonce = secio.readNonce(Flux.from(decrypted))


        val mplexInbound = Flux.from(decrypted)
                .skip(1) //nonce
                .transform(multistream.readProtocol("/mplex/6.7.0"))
        val mplexStarter = Flux.from(mplex.start(mplexInbound))
                .doOnError { log.error("Failed to start Mplex", it) }
        val mplexResponse = respondStandardRequests(mplex)

        val identify = requestIdentify(mplex)
        val dht = requestDht(mplex)
        val peerId = Flux.from(decrypted).next()
                .then(Mono.just(secio).map(Secio::getPeerId))
                .map { Data(DataType.PEER_ID, it) }

        val processorsData: Publisher<Data<*>> = Flux.merge(
                Flux.from(identify.t1).subscribeOn(Schedulers.elastic()),
                Flux.from(dht.t1).subscribeOn(Schedulers.elastic()),
                Flux.from(peerId).subscribeOn(Schedulers.elastic())
        )
        val processorsOutbound = Flux.merge(
                identify.t2, dht.t2
        )

        val main: Publisher<Void> = outbound.send(
                Flux.concat(
                        proposeSecio,
                        establishSecio,
                        receiveSecioNonce.thenMany(mplexStarter)
                                .map(secio.encoder()),
                        Flux.merge(
                                processorsOutbound,
                                mplexResponse
                        ).map(secio.encoder())
                ).map {
                    Unpooled.wrappedBuffer(it)
                }
        )

        return Tuples.of(main, processorsData)
    }


    //
    // ------------------------
    //

    enum class DataType(val clazz: Class<out Any>) {
        IDENTIFY(IdentifyOuterClass.Identify::class.java),
        DHT_NODES(Dht.Message::class.java),
        PEER_ID(PeerId::class.java)
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