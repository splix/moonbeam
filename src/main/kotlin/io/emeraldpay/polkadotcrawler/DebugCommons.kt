package io.emeraldpay.polkadotcrawler

import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufUtil
import org.apache.commons.codec.binary.Hex
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import java.util.function.Function

class DebugCommons {

    companion object {
        private val log = LoggerFactory.getLogger(DebugCommons::class.java)

        fun traceByteBuf(label: String): Function<Flux<ByteBuf>, Flux<ByteBuf>> {
            return Function { flux ->
                flux.doOnNext { trace(label, it) }
            }
        }

        @JvmStatic
        fun trace(label: String, data: ByteBuf) {
            println(">>>>>>>>>>>>>>>> $label <<<<<<<<<<<<<<<<\n" + ByteBufUtil.prettyHexDump(data))
            println("$label hex = ${toHex(data)}")
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