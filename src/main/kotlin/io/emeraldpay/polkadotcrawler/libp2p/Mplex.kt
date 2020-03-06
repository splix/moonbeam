package io.emeraldpay.polkadotcrawler.libp2p

import io.emeraldpay.polkadotcrawler.ByteBufferCommons
import io.emeraldpay.polkadotcrawler.DebugCommons
import org.reactivestreams.Publisher
import org.slf4j.LoggerFactory
import org.springframework.scheduling.concurrent.CustomizableThreadFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import java.nio.ByteBuffer
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong

class Mplex: AutoCloseable {

    companion object {
        private val log = LoggerFactory.getLogger(Mplex::class.java)
        private val EXECUTOR_SUBSCRIPTION = Executors.newFixedThreadPool(4, CustomizableThreadFactory("mplex-sub-"))
        private val VARINT_CONVERTER = SizePrefixed.VarintSize()
    }

    private val seq = AtomicLong(1000)
    private lateinit var input: Flux<Message>

    fun start(input: Flux<ByteBuffer>) {
        this.input = input
                .flatMap {
                    Flux.fromIterable(parse(it))
                }
//                .doOnNext { msg -> DebugCommons.trace("MPLEX ${msg.header.flag} ${msg.header.id}", msg.data, false) }
                .publish().refCount(1, Duration.ofMillis(100))
    }

    override fun close() {
        log.debug("Close Mplex connection")
        //TODO
    }

    fun parse(msg: ByteBuffer): List<Message> {
        val result = ArrayList<Message>(1)
        while (msg.remaining() > 0) {
            try {
                val parsed = Message.decode(msg)
                result.add(parsed)
            } catch (e: Throwable) {
                log.debug("Invalid message. ${e.message}")
            }
        }
        return result
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

    fun newStream(start: Publisher<ByteBuffer>): MplexStream {
        val id = seq.incrementAndGet()
        val stream: Publisher<ByteBuffer> = getMessages(input, id, Flag.MessageReceiver)
        val msg = Message(Header(Flag.NewStream, id), ByteBuffer.wrap("stream $id".toByteArray()))
        return MplexStream(id, true, stream)
                .sendMessage(Mono.just(msg))
                .send(start)
    }

    fun <T> receiveStreams(handler: Handler<T>): Flux<T> {
        val f = Flux.from(input)
                .subscribeOn(Schedulers.fromExecutor(EXECUTOR_SUBSCRIPTION))
                .share().cache(1)
        val result = Flux.from(f)
                .filter {
                    it.header.flag == Flag.NewStream
                }
                .map { init ->
                    val id = init.header.id
                    val inbound: Publisher<ByteBuffer> = getMessages(f, id, Flag.MessageInitiator)
                    val stream = MplexStream(id, false, inbound)
                    return@map handler.handle(stream)
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
                if (input.remaining() < len) {
                    throw IllegalArgumentException("Buffer is shorter than declared length. ${input.remaining()} < $len")
                }
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

            fun message(initiator: Boolean): Flag {
                return if (initiator) {
                    Flag.MessageInitiator
                } else {
                    Flag.MessageReceiver
                }
            }

            fun close(initiator: Boolean): Flag {
                return if (initiator) {
                    Flag.CloseInitiator
                } else {
                    Flag.CloseReceiver
                }
            }

            fun reset(initiator: Boolean): Flag {
                return if (initiator) {
                    Flag.ResetInitiator
                } else {
                    Flag.ResetReceiver
                }
            }
        }
    }


    interface Handler<T> {
        fun handle(stream: MplexStream): T
    }

    class MplexStream(private val streamId: Long, private val initiator: Boolean,
                      val inbound: Publisher<ByteBuffer>) {

        private var closed = false
        private val oubound = ArrayList<Publisher<Message>>()

        fun sendMessage(values: Publisher<Message>): MplexStream {
            oubound.add(values)
            return this
        }

        fun send(values: Publisher<ByteBuffer>): MplexStream {
            oubound.add(Flux.from(values).map {
                Message(Header(Flag.message(initiator), streamId), it)
            })
            return this
        }

        fun outbound(): Flux<ByteBuffer> {
            if (closed) {
                throw IllegalStateException("Mplex outbound is already closed")
            }
            val messages = Flux.concat(oubound)
            val close = Mono.just(Message(Header(Flag.close(initiator), streamId), ByteBuffer.allocate(0)))
            closed = true
            return Flux.concat(messages, close)
//                    .doOnNext { msg -> DebugCommons.trace("MPLEX ${msg.header.flag} ${streamId}", msg.data, true) }
                    .map { it.encode() }
        }

        fun close(): MplexStream {
            closed = true
            return this
        }
    }
}