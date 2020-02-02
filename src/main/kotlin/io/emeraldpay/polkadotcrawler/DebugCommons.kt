package io.emeraldpay.polkadotcrawler

import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufUtil
import org.apache.commons.codec.binary.Hex
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import java.lang.StringBuilder
import java.util.function.Function

class DebugCommons {

    companion object {
        private val log = LoggerFactory.getLogger(DebugCommons::class.java)

        fun traceByteBuf(label: String, out: Boolean): Function<Flux<ByteBuf>, Flux<ByteBuf>> {
            return Function { flux ->
                flux.doOnNext { trace(label, it, out) }
            }
        }

        @JvmStatic
        fun trace(label: String, data: ByteBuf, out: Boolean) {
            val prefix = if (out) {
                "                                                  "
            } else {
                ""
            }
            val buf = StringBuilder()
                    .append("${prefix}         +-------------------------------------------------+").append('\n')
                    .append("${prefix}         | $label").append('\n')
                    .append(ByteBufUtil.prettyHexDump(data).split("\n").map { prefix + it }.joinToString("\n")).append('\n')
                    .append("${prefix}|  HEX   | ${toHex(data)}").append('\n')
                    .append("${prefix}+--------+------------------------------------------------------------------+").append('\n')

            print(buf)
        }

        @JvmStatic
        fun toHex(data: ByteBuf): String {
            return if (data.readableBytes() > 0 || data.readerIndex() > 0) {
                val slice = data.slice(0, data.readerIndex() + data.readableBytes())
                val copy = ByteArray(slice.readableBytes())
                slice.readBytes(copy)
                Hex.encodeHexString(copy)
            } else {
                "0x"
            }
        }
    }

}