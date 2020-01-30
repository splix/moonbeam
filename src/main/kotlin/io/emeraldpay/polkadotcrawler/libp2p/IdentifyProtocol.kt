package io.emeraldpay.polkadotcrawler.libp2p

import identify.pb.IdentifyOuterClass
import io.emeraldpay.polkadotcrawler.DebugCommons
import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufInputStream
import org.slf4j.LoggerFactory

class IdentifyProtocol {

    companion object {
        private val log = LoggerFactory.getLogger(IdentifyProtocol::class.java)
    }

    fun parse(data: ByteBuf): IdentifyOuterClass.Identify {
//        DebugCommons.trace("PARSE ID", data)
        return IdentifyOuterClass.Identify.parseFrom(ByteBufInputStream(data))
    }

}