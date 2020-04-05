package io.emeraldpay.moonbeam.polkadot

import org.slf4j.LoggerFactory

/**
 * SCALE codec. See https://substrate.dev/docs/en/conceptual/core/codec
 *
 * WARNING: it's not a full featured implementations of the codec. It supports only subset specific to the bot.
 */
class ScaleCodec(
        private val value: ByteArray
) {

    companion object {
        private val log = LoggerFactory.getLogger(ScaleCodec::class.java)
    }

    /**
     * Parser for encoded values
     */
    class Reader(private val value: ByteArray) {
        private var pos = 0;

        fun getByte(): Byte {
            return value[pos++]
        }

        fun getUByte(): Int {
            val x = getByte()
            if (x < 0) {
                return 256 + x.toInt()
            }
            return x.toInt()
        }

        fun getUint16(): Int {
            var result: Int = 0
            result += getUByte()
            result += getUByte().shl(8)
            return result
        }

        fun getUint32(): Long {
            var result: Long = 0
            result += getUByte().toLong()
            result += getUByte().toLong().shl(1 * 8)
            result += getUByte().toLong().shl(2 * 8)
            result += getUByte().toLong().shl(3 * 8)
            return result
        }

        fun getCompactInt(): Int {
            val i = getUByte()
            val mode = CompactMode.byValue(i.and(0b11).toByte())
            if (mode == CompactMode.SINGLE) {
                return i.shr(2)
            }
            if (mode == CompactMode.TWO) {
                return i.shr(2) + getUByte().shl(6)
            }
            if (mode == CompactMode.FOUR) {
                return i.shr(2) +
                        getUByte().shl(6) +
                        getUByte().shl(6 + 8) +
                        getUByte().shl(6 + 2 * 8)
            }
            throw NotImplementedError("Mode $mode is not implemented")
        }

        fun getByteArray(): ByteArray {
            val len = getCompactInt()
            return getByteArray(len)
        }

        fun getByteArray(len: Int): ByteArray {
            val result = ByteArray(len)
            (0 until len).forEachIndexed { index, _ ->
                result[index] = getByte()
            }
            return result
        }

    }

    enum class CompactMode(private val value: Byte) {
        SINGLE(0b00),
        TWO(0b01),
        FOUR(0b10),
        BIGINT(0b11);

        companion object {
            fun byValue(value: Byte): CompactMode {
                return if (value == SINGLE.value) {
                    SINGLE
                } else if (value == TWO.value) {
                    TWO
                } else if (value == FOUR.value) {
                    FOUR
                } else {
                    BIGINT
                }
            }
        }
    }
}