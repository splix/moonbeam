package io.emeraldpay.polkadotcrawler.libp2p

import com.google.protobuf.CodedOutputStream
import io.emeraldpay.polkadotcrawler.ByteBufferCommons
import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufUtil
import io.netty.buffer.Unpooled
import io.netty.util.ReferenceCountUtil
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.nio.ByteBuffer
import java.util.function.Function
import java.util.function.Predicate
import kotlin.streams.toList

class Multistream {

    companion object {
        private val log = LoggerFactory.getLogger(Multistream::class.java)
        private val NAME = "/multistream/1.0.0"
        private val LEN_PREFIX_SIZE = 1;
        private val NL_SIZE = 1;

        private val sizePrefix = SizePrefixed.Varint().prefix
    }

    fun headerFor(protocol: String): ByteBuffer {
        val size1 = sizePrefix.write(NAME.length + 1)
        val size2 = sizePrefix.write(protocol.length + 1)

        val result = ByteBuffer.allocate(NAME.length + protocol.length + 2 + size1.remaining() + size2.remaining())
        result.put(size1)
        result.put(NAME.toByteArray())
        result.put('\n'.toByte())
        result.put(size2)
        result.put(protocol.toByteArray())
        result.put('\n'.toByte())

        return result.flip()
    }

    fun readProtocol(protocol: String, includesSizePrefix: Boolean = true, onFound: Mono<Void>? = null): Function<Flux<ByteBuffer>, Flux<ByteBuffer>> {
        var headerBuffer: ByteBuffer? = null

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

        var headerFound = false

        val allRead: Predicate<ByteBuffer> = Predicate { input ->
            if (headerFound) {
                true
            } else {
                if (input.hasRemaining()) {
                    headerBuffer = if (headerBuffer == null) {
                        input
                    } else {
                        ByteBufferCommons.join(headerBuffer!!, input)
                    }
                }
                headerFound = headerBuffer != null && headerBuffer!!.remaining() >= headerSize
                headerFound
            }
        }
        return Function { flux ->
            flux.bufferUntil(allRead)
                    .flatMap { list ->
                        if (headerBuffer == null) {
                            //list always has 1 element after header was found
                            Flux.fromIterable(list)
                        } else {
                            val ref = headerBuffer!!
                            val result: ByteBuffer
                            headerBuffer = null
                            val expectedHeader: ByteBuffer = exp.flip()
                            val actualHeader: ByteBuffer = ByteBufferCommons.copy(ref, headerSize)

                            if (actualHeader != expectedHeader) {
                                System.err.println("Actual:\n" + ByteBufUtil.prettyHexDump(Unpooled.wrappedBuffer(actualHeader)))
                                System.err.println("Expected:\n" + ByteBufUtil.prettyHexDump(Unpooled.wrappedBuffer(expectedHeader)))
                                return@flatMap Mono.error<ByteBuffer>(IllegalStateException("Received invalid header"))
                            }
                            result = ByteBufferCommons.copy(ref, ref.remaining())
                            if (onFound != null) {
                                onFound.thenReturn(result)
                            } else {
                                Mono.just(result)
                            }
                        }
                    }
                    .filter {
                        // skip if header took whole space
                        it.remaining() > 0
                    }

        }
    }

}