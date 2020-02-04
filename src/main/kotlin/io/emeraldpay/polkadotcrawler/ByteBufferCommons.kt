package io.emeraldpay.polkadotcrawler

import org.slf4j.LoggerFactory
import java.nio.ByteBuffer

class ByteBufferCommons {

    companion object {
        private val log = LoggerFactory.getLogger(ByteBufferCommons::class.java)

        @JvmStatic
        fun join(vararg items: ByteBuffer): ByteBuffer {
            val len = items.sumBy { it.remaining() }
            val result = ByteBuffer.allocate(len)
            items.forEach { source ->
                result.put(source)
            }
            return result.flip()
        }

        fun copy(buf: ByteBuffer, length: Int): ByteBuffer {
            val result = ByteArray(length)
            buf.get(result)
            return ByteBuffer.wrap(result)
        }

        fun contains(buf: ByteBuffer, value: ByteArray): Boolean {
            buf.mark()
            for (i in buf.position()..(buf.position()+buf.remaining()-value.size)) {
                var j = 0
                var found = true
                while (found && j < value.size) {
                    found = buf.get(i+j) == value[j]
                    j++
                }
                if (found) {
                    buf.reset()
                    return true
                }
            }
            buf.reset()
            return false
        }

    }

}