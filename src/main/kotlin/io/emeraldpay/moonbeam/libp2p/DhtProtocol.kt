package io.emeraldpay.moonbeam.libp2p

import com.google.protobuf.ByteString
import io.emeraldpay.moonbeam.proto.Dht
import io.libp2p.core.PeerId
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import java.nio.ByteBuffer


class DhtProtocol() {

    companion object {
        private val log = LoggerFactory.getLogger(DhtProtocol::class.java)
        private const val REQUESTS = 32
    }

    private val sizePrefixed = SizePrefixed.Varint()

    fun start(): Flux<ByteBuffer> {
        return Flux.merge(
                Flux.range(0, REQUESTS).map {
                    requestNodes(it)
                },
                Flux.range(0, REQUESTS).map {
                    requestProviders(it)
                }
        )
    }

    fun requestNodes(i: Int): ByteBuffer {
        val key = PeerId.random()
        val msg = Dht.Message.newBuilder()
                .setType(Dht.Message.MessageType.FIND_NODE)
                .setKey(ByteString.copyFrom(key.bytes))
                .build()
        return sizePrefixed.write(ByteBuffer.wrap(msg.toByteArray()))
    }

    fun requestProviders(i: Int): ByteBuffer {
        val key = PeerId.random()
        val msg = Dht.Message.newBuilder()
                .setType(Dht.Message.MessageType.GET_PROVIDERS)
                .setKey(ByteString.copyFrom(key.bytes))
                .build()
        return sizePrefixed.write(ByteBuffer.wrap(msg.toByteArray()))
    }

    fun parse(data: ByteBuffer): Dht.Message {
//        DebugCommons.trace("PARSE DHT", it)
        return Dht.Message.parseFrom(
                data.array()
        )
    }

}