package io.emeraldpay.polkadotcrawler.libp2p

import com.google.protobuf.CodedOutputStream
import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufUtil
import io.netty.buffer.Unpooled
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import java.nio.ByteBuffer
import java.util.function.Function
import java.util.function.Predicate

class Multistream {

    companion object {
        private val log = LoggerFactory.getLogger(Multistream::class.java)
        private val NAME = "/multistream/1.0.0"
        private val LEN_PREFIX_SIZE = 1;
        private val NL_SIZE = 1;
    }

    fun headerFor(protocol: String): ByteBuf {
        return listOf(NAME, protocol).stream()
                .map { it + "\n" }
                .map { it.toByteArray() }
                .flatMap {
                    val buf = ByteArray(CodedOutputStream.computeUInt32SizeNoTag(it.size))
                    CodedOutputStream.newInstance(buf).writeInt32NoTag(it.size)
                    listOf(
                            buf,
                            it
                    ).stream()
                }
                .map {
                    Unpooled.wrappedBuffer(it)
                }
                .reduce { a: ByteBuf, b: ByteBuf -> Unpooled.wrappedBuffer(a, b) }
                .get()
    }

    fun write(value: ByteBuf, protocol: String): ByteBuf {
        val header = headerFor(protocol)
        val body = SizePrefixed.Standard().write(value)
        return Unpooled.wrappedBuffer(
                header, body
        )
    }

    fun readProtocol(protocol: String, includesSizePrefix: Boolean = true): Function<Flux<ByteBuf>, Flux<ByteBuf>> {
        var headerBuffer: ByteBuf? = null

        var headerSize = NAME.length + NL_SIZE + protocol.length + NL_SIZE

        if (includesSizePrefix) {
            headerSize += LEN_PREFIX_SIZE * 2
        }

        val exp = ByteBuffer.allocate(headerSize)
        if (includesSizePrefix) {
            exp.put((NAME.length + 1).toByte())
        }
        exp.put(NAME.toByteArray())
        exp.put("\n".toByteArray())
        if (includesSizePrefix) {
            exp.put((protocol.length + 1).toByte())
        }
        exp.put(protocol.toByteArray())
        exp.put("\n".toByteArray())
        exp.array()

        var headerFound = false

        val allRead: Predicate<ByteBuf> = Predicate {
            if (headerFound) {
                true
            } else {
                val copy = it.slice()
                if (headerBuffer == null) {
                    headerBuffer = copy.copy()
                } else {
                    headerBuffer = Unpooled.wrappedBuffer(headerBuffer, it)
                }
                headerFound = headerBuffer!!.readableBytes() >= headerSize
                headerFound
            }
        }
        return Function { flux ->
            flux.bufferUntil(allRead)
                    .map {
                        if (headerBuffer == null) {
                            //list always has 1 element after header was found
                            it.first()
                        } else {
                            val ref = headerBuffer!!
                            headerBuffer = null

                            if (ref.readableBytes() < headerSize) {
                                throw IllegalStateException("Insufficient data. Expect $headerSize, have ${ref.readableBytes()}")
                            }
                            if (ref.slice(0, headerSize) != Unpooled.wrappedBuffer(exp.array())) {
                                println("Actual:\n" + ByteBufUtil.prettyHexDump(ref.slice(0, headerSize)))
                                println("Expected:\n" + ByteBufUtil.prettyHexDump(Unpooled.wrappedBuffer(exp.array())))
                                throw IllegalStateException("Received invalid header")
                            }
                            ref.slice(headerSize, ref.readableBytes() - headerSize)
                        }
                    }
                    .filter {
                        // skip if header took whole space
                        it.readableBytes() > 0
                    }

        }
    }

}