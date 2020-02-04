package io.emeraldpay.polkadotcrawler

import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufUtil
import io.netty.buffer.Unpooled
import org.apache.commons.codec.binary.Hex
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import java.lang.StringBuilder
import java.nio.ByteBuffer
import java.util.function.Function

class DebugCommons {

    companion object {
        private val log = LoggerFactory.getLogger(DebugCommons::class.java)

        fun traceByteBuf(label: String, out: Boolean): Function<Flux<ByteBuffer>, Flux<ByteBuffer>> {
            return Function { flux ->
                flux.doOnNext { trace(label, it, out) }
            }
        }

        @JvmStatic
        fun trace(label: String, data: ByteBuffer, out: Boolean) {
            val prefix = if (out) {
                "                                                  "
            } else {
                ""
            }
            val buf = StringBuilder()
                    .append("${prefix}         +-------------------------------------------------+").append('\n')
                    .append("${prefix}         | $label").append('\n')
                    .append(ByteBufUtil.prettyHexDump(Unpooled.wrappedBuffer(data)).split("\n").map { prefix + it }.joinToString("\n")).append('\n')
                    .append("${prefix}|  HEX   | ${toHex(data)}").append('\n')
                    .append("${prefix}+--------+------------------------------------------------------------------+").append('\n')

            print(buf)
        }

        @JvmStatic
        fun toHex(data: ByteBuffer): String {
            return if (data.remaining() > 0 || data.position() > 0) {
                data.mark()
                val copy = ByteArray(data.remaining())
                data.get(copy)
                data.reset()
                Hex.encodeHexString(copy)
            } else {
                "0x"
            }
        }
    }

}