package io.emeraldpay.polkadotcrawler.libp2p

import com.google.protobuf.CodedInputStream
import com.google.protobuf.CodedOutputStream
import com.google.protobuf.InvalidProtocolBufferException
import io.emeraldpay.polkadotcrawler.ByteBufferCommons
import io.emeraldpay.polkadotcrawler.DebugCommons
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import java.io.ByteArrayOutputStream
import java.nio.BufferUnderflowException
import java.nio.ByteBuffer
import java.util.function.Function
import java.util.function.Predicate
import kotlin.math.min

class SizePrefixed {

    companion object {
        private val log = LoggerFactory.getLogger(SizePrefixed::class.java)

        @JvmStatic
        fun Standard(): Converter {
            return Converter(StandardSize())
        }

        @JvmStatic
        fun Varint(): Converter {
            return Converter(VarintSize())
        }
    }

    class Converter(val prefix: SizePrefix<Int>) {

        fun scanForExpected(data: ByteBuffer): Int {
            if (data.remaining() == 0) {
                return 0
            }
            val pos = data.position()
            val len = try {
                prefix.read(data)
            } catch (e: BufferUnderflowException) {
                data.position(pos)
                // for Standard can calculate exact
                return 4 - data.remaining()
            } catch (e: InvalidProtocolBufferException) {
                // Error Message: While parsing a protocol message, the input ended unexpectedly in the middle of a field.  This could mean either that the input has been truncated or that an embedded message misreported its own length.
                // for Varint needs at least one byte
                data.position(pos)
                return data.remaining() + 1
            }
            if (len < 0) {
                throw IllegalStateException("Invalid len: $len")
            }
            if (data.remaining() < len) {
                return len - data.remaining()
            }
            return scanForExpected(data.position(data.position() + len))
        }

        fun isFullyRead(): Predicate<ByteBuffer> {
            var expect = 0
            return Predicate {
                var copy = it.slice()
                if (expect > 0) {
                    val len = min(expect, copy.remaining())
                    if (len == 0) {
                        throw IllegalStateException("0 to read")
                    }
                    copy = copy.position(copy.position() + len)
                    expect -= len
                    if (expect == 0) {
                        expect = scanForExpected(copy)
                    }
                } else {
                    expect = scanForExpected(copy)
                }
                return@Predicate expect == 0
            }
        }

        fun split(data: ByteBuffer): List<ByteBuffer> {
            if (Unpooled.EMPTY_BUFFER == data) {
                return emptyList()
            }

            val result = ArrayList<ByteBuffer>()
            while (data.remaining() > 0) {
                var len = prefix.read(data)
                if (data.remaining() < len) {
                    log.warn("Have less than expected. Have ${data.remaining()} < $len requested (as ${DebugCommons.toHex(prefix.write(len))})")
                    len = data.remaining()
                }
                val actualData = ByteBufferCommons.copy(data, len)
                result.add(actualData)
            }

            return result
        }

        fun reader(): Function<Flux<ByteBuffer>, Flux<ByteBuffer>> {
            return Function { flux ->
                flux.bufferUntil(isFullyRead())
                        .map {  list ->
                            if (list.size == 1) {
                                list.first()
                            } else {
                                ByteBufferCommons.join(*list.toTypedArray())
                            }
                        }
                        .flatMap {
                            Flux.fromIterable(split(it))
                        }
                        .filter {
                            it.remaining() > 0
                        }
            }
        }

        fun write(bytes: ByteBuffer): ByteBuffer {
            return ByteBufferCommons.join(prefix.write(bytes.remaining()), bytes)
        }

        fun writer(): Function<Flux<ByteBuffer>, Flux<ByteBuffer>> {
            return Function { flux ->
                flux.map { bytes -> write(bytes) }
            }
        }
    }

    interface SizePrefix<T: Number> {
        fun read(input: ByteBuffer): T
        fun write(value: T): ByteBuffer
    }

    class StandardSize(): SizePrefix<Int> {
        override fun read(input: ByteBuffer): Int {
            return input.getInt()
        }

        override fun write(value: Int): ByteBuffer {
            return ByteBuffer.allocate(4).putInt(value).flip()
        }

    }

    class VarintSize(): SizePrefix<Int> {
        override fun read(input: ByteBuffer): Int {
            val coded = CodedInputStream.newInstance(input)
            val result = coded.readUInt32()
            input.position(input.position() + coded.totalBytesRead)
            return result
        }

        override fun write(value: Int): ByteBuffer {
            val buf = ByteArrayOutputStream()
            val input = CodedOutputStream.newInstance(buf)
            input.writeUInt32NoTag(value)
            input.flush()
            return ByteBuffer.wrap(buf.toByteArray())
        }
    }

    class VarlongSize(): SizePrefix<Long> {
        override fun read(input: ByteBuffer): Long {
            val coded = CodedInputStream.newInstance(input)
            val result = coded.readUInt64()
            input.position(input.position() + coded.totalBytesRead)
            return result
        }

        override fun write(value: Long): ByteBuffer {
            val buf = ByteArrayOutputStream()
            val input = CodedOutputStream.newInstance(buf)
            input.writeUInt64NoTag(value)
            input.flush()
            return ByteBuffer.wrap(buf.toByteArray())
        }
    }
}