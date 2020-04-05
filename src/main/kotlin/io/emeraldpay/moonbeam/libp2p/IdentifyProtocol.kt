package io.emeraldpay.moonbeam.libp2p

import identify.pb.IdentifyOuterClass
import io.emeraldpay.moonbeam.DebugCommons
import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufInputStream
import org.slf4j.LoggerFactory
import java.nio.ByteBuffer

class IdentifyProtocol {

    companion object {
        private val log = LoggerFactory.getLogger(IdentifyProtocol::class.java)
    }

    fun parse(data: ByteBuffer): IdentifyOuterClass.Identify {
//        DebugCommons.trace("PARSE ID", data)
        return IdentifyOuterClass.Identify.parseFrom(data.array())
    }

}