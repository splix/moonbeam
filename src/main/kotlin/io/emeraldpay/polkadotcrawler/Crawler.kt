package io.emeraldpay.polkadotcrawler

import com.google.protobuf.ByteString
import identify.pb.IdentifyOuterClass
import io.emeraldpay.polkadotcrawler.crawler.CrawlerClient
import io.emeraldpay.polkadotcrawler.crawler.CrawlerData
import io.emeraldpay.polkadotcrawler.crawler.ExternalIp
import io.emeraldpay.polkadotcrawler.discover.Discovered
import io.emeraldpay.polkadotcrawler.discover.NoRecentChecks
import io.emeraldpay.polkadotcrawler.discover.PublicPeersOnly
import io.emeraldpay.polkadotcrawler.export.FileJsonExport
import io.emeraldpay.polkadotcrawler.export.MysqlExport
import io.emeraldpay.polkadotcrawler.monitoring.Monitoring
import io.emeraldpay.polkadotcrawler.polkadot.StatusProtocol
import io.emeraldpay.polkadotcrawler.processing.FullProcessor
import io.emeraldpay.polkadotcrawler.proto.Dht
import io.emeraldpay.polkadotcrawler.state.PeerDetails
import io.libp2p.core.PeerId
import io.libp2p.core.crypto.*
import io.libp2p.core.multiformats.Multiaddr
import org.apache.commons.codec.binary.Hex
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import reactor.extra.processor.TopicProcessor
import reactor.netty.tcp.TcpServer
import java.time.Duration
import java.util.function.BiFunction

@Service
class Crawler(
        @Autowired private val discovered: Discovered,
        @Autowired private val noRecentChecks: NoRecentChecks,
        @Autowired private val fileJsonExport: FileJsonExport,
        @Autowired private val mysqlExport: MysqlExport,
        @Autowired private val monitoring: Monitoring,
        @Autowired private val processor: FullProcessor,
        @Value("\${port}") private val port: Int,
        @Value("\${key:RANDOM}") private val key: String
): Runnable {

    companion object {
        private val log = LoggerFactory.getLogger(Crawler::class.java)
    }

    private val keys: Pair<PrivKey, PubKey>
    private var externalIp: String? = null
    private val agent: IdentifyOuterClass.Identify
    private val publicPeersOnly = PublicPeersOnly()
    private val connected = TopicProcessor.create<PeerDetails>()

    init {
        if (key == "RANDOM") {
            keys = generateKeyPair(KEY_TYPE.ED25519)
            log.warn("Using new random key for the bot. It's recommended to reuse private key between restarts by specifying --key=PRIVATE_KEY_HEX")
            log.warn("To reuse current key specify it as --key=${Hex.encodeHexString(marshalPrivateKey(keys.first))}")
        } else {
            val private = unmarshalPrivateKey(Hex.decodeHex(key))
            keys = Pair(private, private.publicKey())
        }
        log.info("Public key ${Hex.encodeHexString(keys.second.raw())}")
        externalIp = ExternalIp().requestIp()
        if (externalIp == null) {
            log.warn("External IP not discovered")
        } else {
            log.info("Listen on 0.0.0.0:${port}, external address ${externalIp}:${port}")
            listOf(externalIp, "127.0.0.1").forEach { address ->
                log.info("Address: ${Multiaddr(Multiaddr.fromString("/ip4/$address/tcp/$port"), PeerId.fromPubKey(keys.second)).toString().replace("/ipfs/", "/p2p/")}")
            }
        }
        agent = IdentifyOuterClass.Identify.newBuilder()
                .setAgentVersion("substrate-bot/0.2.0")
                .setProtocolVersion("/substrate/1.0")
                .addProtocols("/substrate/ksmcc3/6")
                .addProtocols("/ipfs/ping/1.0.0")
                .addProtocols("/ipfs/id/1.0.0")
                .addProtocols("/ipfs/kad/1.0.0")
                .addListenAddrs(
                        ByteString.copyFrom(
                                Multiaddr(
                                        Multiaddr.fromString("/ip4/127.0.0.1/tcp/$port"),
                                        PeerId.fromPubKey(keys.second)
                                ).getBytes()
                        )
                )
                .let {
                    externalIp?.let { ip ->
                        it.addListenAddrs(
                                ByteString.copyFrom(
                                        Multiaddr(
                                                Multiaddr.fromString("/ip4/$ip/tcp/$port"),
                                                PeerId.fromPubKey(keys.second)
                                        ).getBytes()
                                )
                        )
                    }
                    it
                }
                .build()
    }

    override fun run() {
        Flux.from(discovered.listen())
                .subscribeOn(Schedulers.newSingle("crawler"))
                .filter(noRecentChecks)
                .flatMap({
                    connect(it).onErrorResume { t ->
                        if (t is NotLoadedException) {
                            log.debug("Peer not loaded. ${t.peer.address}")
                            noRecentChecks.forget(t.peer.address)
                        } else {
                            log.warn("Failed to connect", t)
                        }
                        Mono.empty()
                    }.subscribeOn(Schedulers.elastic())
                }, 32)
                .subscribe(connected)

        val result = Flux.from(connected)
                .subscribeOn(Schedulers.newSingle("connected"))
                .doOnNext { monitoring.onPeerProcessed(it) }
                .map(processor)
                .doOnError { it.printStackTrace() }
                .share()

        result.subscribe(fileJsonExport)
        result.subscribe(mysqlExport.getInstance())

        server()
    }

    fun readFromPeer(address: Multiaddr): BiFunction<PeerDetails, CrawlerData.Value<*>, PeerDetails> {
        return BiFunction { details, it ->
            log.debug("Received ${it.dataType} from $address")
            when (it.dataType) {
                CrawlerData.Type.DHT_NODES -> {
                    val dht = it.cast(Dht.Message::class.java)
                    details.add(dht.data)

                    listOf(dht.data.closerPeersList, dht.data.providerPeersList).forEach { peers ->
                        peers.flatMap {
                            it.addrsList
                        }.mapNotNull {
                            try {
                                Multiaddr(it.toByteArray())
                            } catch (e: java.lang.IllegalArgumentException) {
                                log.debug("Invalid address")
                                null
                            }
                        }.filter {
                            publicPeersOnly.test(it)
                        }.forEach {
                            discovered.submit(it)
                        }
                    }
                }

                CrawlerData.Type.IDENTIFY -> {
                    val id = it.cast(IdentifyOuterClass.Identify::class.java)
                    details.add(id.data)
                }

                CrawlerData.Type.PEER_ID -> {
                    details.peerId = it.cast(PeerId::class.java).data
                }

                CrawlerData.Type.STATUS -> {
                    details.status = it.cast(StatusProtocol.Status::class.java).data
                }

                CrawlerData.Type.PROTOCOLS -> {
                    details.protocols = it.cast(CrawlerData.StringList::class.java).data.values
                }
            }

            details
        }
    }


    fun connect(address: Multiaddr): Mono<PeerDetails> {
        try {
            var crawler: CrawlerClient? = null
            val result = Mono.just(address)
                    .map { CrawlerClient(it, agent, keys) }
                    .doOnNext { crawler = it }
                    .flatMapMany { it.connect() }
                    .take(Duration.ofSeconds(15))
                    .doFinally { crawler?.disconnect() }
                    .reduce(PeerDetails(address, false), readFromPeer(address))
                    .doOnNext { it.close() }
                    .onErrorResume {
                        log.debug("Connection failure. ${it.javaClass}: ${it.message}")
                        Mono.empty()
                    }
                    .map {
                        if (!it.filled()) {
                            throw NotLoadedException(it)
                        }
                        it
                    }
            return result
        } catch (e: Exception) {
            log.error("Failed to setup crawler connection", e)
            throw e
        }

    }

    fun server(): Mono<Void> {
        val server = TcpServer.create()
                .host("0.0.0.0")
                .port(port)
                .handle { inbound, outbound ->
                    var remote: Multiaddr? = null
                    inbound.withConnection {
                        val remoteAddress = it.address().address.hostAddress
                        val remotePort = it.address().port
                        remote = Multiaddr.fromString("/ip4/$remoteAddress/tcp/$remotePort")
                    }
                    if (remote == null) {
                        log.warn("Remote address is unknown")
                        return@handle Mono.empty<Void>()
                    }
                    remote?.let { address ->
                        log.debug("Connection from $address")
                        val crawler = CrawlerClient(address, agent, keys)
                        val result = crawler.handle(inbound, outbound, false)
                        Flux.from(result.t2)
                                .take(Duration.ofSeconds(60))
                                .reduce(PeerDetails(address, true), readFromPeer(address))
                                .filter { it.filled() }
                                .subscribe(connected)
                        result.t1
                    }
                }
                .bindNow()

        return server.onDispose()
    }


    class NotLoadedException(val peer: PeerDetails): Exception()
}