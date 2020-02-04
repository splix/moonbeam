package io.emeraldpay.polkadotcrawler.libp2p

import com.google.protobuf.ByteString
import io.emeraldpay.polkadotcrawler.proto.Dht
import io.libp2p.core.PeerId
import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufInputStream
import io.netty.buffer.Unpooled
import org.reactivestreams.Publisher
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import java.nio.ByteBuffer


class DhtProtocol {

    companion object {
        private val log = LoggerFactory.getLogger(DhtProtocol::class.java)
    }

    private val sizePrefixed = SizePrefixed.Varint()

    fun start(): Flux<ByteBuffer> {
        return Flux.range(0, 3).map {
            request()
        }
    }

    fun request(): ByteBuffer {
        val key = PeerId.random()
        val msg = Dht.Message.newBuilder()
                .setType(Dht.Message.MessageType.FIND_NODE)
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