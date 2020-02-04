package io.emeraldpay.polkadotcrawler.libp2p

import io.emeraldpay.polkadotcrawler.ByteBufferCommons
import io.emeraldpay.polkadotcrawler.DebugCommons
import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufHolder
import io.netty.buffer.DefaultByteBufHolder
import io.netty.buffer.Unpooled
import org.reactivestreams.Publisher
import org.slf4j.LoggerFactory
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.extra.processor.TopicProcessor
import java.io.Closeable
import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.util.concurrent.atomic.AtomicLong

class Mplex: AutoCloseable {

    companion object {
        private val log = LoggerFactory.getLogger(Mplex::class.java)

        private val VARINT_CONVERTER = SizePrefixed.VarintSize()
    }

    private val multistream = Multistream()
    private val seq = AtomicLong(1000);
    private val messages = TopicProcessor.create<Message>()
    private val outbound = TopicProcessor.create<ByteBuffer>()

    fun start(): Publisher<ByteBuffer> {
        val starter = multistream.headerFor("/mplex/6.7.0")
        return Flux.concat(Mono.just(starter), outbound)
    }

    override fun close() {
        log.debug("Close Mplex connection")
        messages.dispose()
        outbound.dispose()
    }

    fun parse(msg: ByteBuffer): List<Message> {
        val result = ArrayList<Message>(1)
        while (msg.remaining() > 0) {
            val parsed = Message.decode(msg)
            result.add(parsed)
        }
        return result
    }

    fun onNext(input: ByteBuffer) {
        try {
            parse(input).forEachIndexed { i, msg ->
//                val ascii = msg.content().toString(Charset.defaultCharset())
//                        .replace(Regex("[^\\w/\\\\.-]"), ".")
//                        .toCharArray().joinToString(" ")
//                val hex = DebugCommons.toHex(msg.content())
//                DebugCommons.trace("MPLEX ${msg.header.flag} ${msg.header.id}", msg.content(), false)
//                log.debug("mplex message $i ${msg.header.flag} ${msg.header.id}")
//                log.debug("      $hex")
//                log.debug("      $ascii")

                messages.onNext(msg)
            }
        } catch (e: java.lang.IllegalArgumentException) {
            log.warn("Invalid Mplex data")
        }
    }

    private fun getMessages(source: Flux<Message>, id: Long, flag: Flag): Flux<ByteBuffer> {
        return Flux.from(source)
                .filter {
                    it.header.id == id && it.header.flag == flag
                }
                .map {
                    it.data
                }
    }

    fun <T> newStream(handler: Handler<T>): T {
        val id = seq.incrementAndGet()
        val stream: Publisher<ByteBuffer> = getMessages(Flux.from(messages), id, Flag.MessageReceiver)
        val msg = Message(Header(Flag.NewStream, id), ByteBuffer.wrap("stream $id".toByteArray()))
        outbound.onNext(msg.encode())
        return handler.handle(id, stream, MplexOutbound(id, true, outbound))
    }

    fun <T> receiveStreams(handler: Handler<T>): Flux<T> {
        val f = Flux.from(messages).share().cache(1)
        val result = Flux.from(f)
                .filter {
                    it.header.flag == Flag.NewStream
                }
                .map { init ->
                    val id = init.header.id
                    val stream: Publisher<ByteBuffer> = getMessages(f, id, Flag.MessageInitiator)
                    val outbound = MplexOutbound(id, false, outbound)
                    Flux.from(f)
                            .filter {
                                it.header.id == id && it.header.flag == Flag.CloseInitiator
                            }
                            .single()
                            .subscribe {
                                log.debug("Close stream $id")
                                outbound.close()
                            }
                    return@map handler.handle(id, stream, outbound)
                }
        return result
    }

    class Header(val flag: Flag, val id: Long) {
        fun encode(): ByteBuffer {
            val value = id.shl(3) + flag.id
            return converter.write(value)
        }

        companion object {
            private val converter = SizePrefixed.VarlongSize()

            fun decode(input: ByteBuffer): Header {
                val value = converter.read(input)
                val flagId = value.and(0x07);
                val id = value.shr(3)
                return Header(Flag.byId(flagId.toInt()), id)
            }
        }
    }

    class Message(val header: Header, val data: ByteBuffer) {
        fun encode(): ByteBuffer {
            val header = header.encode()
            val length = VARINT_CONVERTER.write(this.data.remaining())
            return ByteBufferCommons.join(header, length, this.data)
        }

        companion object {
            private val converter = SizePrefixed.VarintSize()

            fun decode(input: ByteBuffer): Message {
                val header = Header.decode(input)
                val len = VARINT_CONVERTER.read(input)
                val data = ByteArray(len)
                input.get(data)
                return Message(header, ByteBuffer.wrap(data))
            }
        }
    }

    enum class Flag(val id: Int) {
        NewStream( 0 ),
        MessageReceiver( 1 ),
        MessageInitiator( 2 ),
        CloseReceiver( 3 ),
        CloseInitiator( 4 ),
        ResetReceiver( 5 ),
        ResetInitiator( 6 );

        companion object {
            fun byId(id: Int): Flag {
                return Flag.values().find { it.id == id } ?: throw IllegalArgumentException("Invalid flag id: $id")
            }
        }
    }

    class MplexOutbound(val streamId: Long, val initiator: Boolean, private val outbound: TopicProcessor<ByteBuffer>): AutoCloseable {

        private val flag = if (initiator) {
            Flag.MessageInitiator
        } else {
            Flag.MessageReceiver
        }

        private val closeFlag = if (initiator) {
            Flag.CloseInitiator
        } else {
            Flag.CloseReceiver
        }

        fun send(value: Publisher<ByteBuffer>): Mono<Void> {
            return Flux.from(value)
                    .map {
                        val msg = Message(Header(flag, streamId), it)
                        msg.encode()
                    }
//                    .transform(DebugCommons.traceByteBuf("MPLEX ${flag} ${streamId}", true))
                    .doOnNext {
                        outbound.onNext(it)
                    }
                    .then()
        }

        override fun close() {
            log.debug("Close stream $streamId")
            val msg = Message(Header(closeFlag, streamId), ByteBuffer.allocate(0)).encode()
            outbound.onNext(msg)
        }
    }

    interface Handler<T> {
        fun handle(id: Long, inboud: Publisher<ByteBuffer>, outboud: MplexOutbound): T
    }
}