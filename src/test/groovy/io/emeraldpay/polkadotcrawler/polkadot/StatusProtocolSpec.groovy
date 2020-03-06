package io.emeraldpay.polkadotcrawler.polkadot

import org.apache.commons.codec.binary.Hex
import spock.lang.Specification

import java.nio.ByteBuffer

class StatusProtocolSpec extends Specification {

    def "Parse standard msg"() {
        setup:
        def msg = Hex.decodeHex("000600000003000000017d010000bb931fd17f85fb26e8209eb7af5747258163df29a7dd8f87fa7617963fcfa1aab0a8d493285c2df73290dfb7e61f870f17b41801197a149ca93654499ea3dafe0400")
        def protocol = new StatusProtocol()

        when:
        def act = protocol.parse(ByteBuffer.wrap(msg))

        then:
        act.height == 381
        Hex.encodeHexString(act.bestHash) == "bb931fd17f85fb26e8209eb7af5747258163df29a7dd8f87fa7617963fcfa1aa"
        Hex.encodeHexString(act.genesis) == "b0a8d493285c2df73290dfb7e61f870f17b41801197a149ca93654499ea3dafe"

    }
}
