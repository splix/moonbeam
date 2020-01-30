package io.emeraldpay.polkadotcrawler.libp2p

import io.netty.buffer.Unpooled
import org.apache.commons.codec.binary.Hex
import spock.lang.Specification

class IdentifyProtocolSpec extends Specification {

    def "Parse message"() {
        setup:
        def hex = "2a0e2f7375627374726174652f312e30323b7061726974792d706f6c6b61646f742f76302e372e31382d66633130306536352d7838365f36342d6c696e75782d676e752028756e6b6e6f776e290a240801122042130f7467bf5bf6c68677a986615f8d5490b2ea23d96f8f7e6614b3de566a54121d36187032702e6363332d332e6b7573616d612e6e6574776f726b06759412080423b2c350067594120804030a87f20675941208047f0000010675941208040a00010f0675942208040a00018606d4d21a132f7375627374726174652f6b736d6363332f351a132f7375627374726174652f6b736d6363332f341a132f7375627374726174652f6b736d6363332f331a102f697066732f70696e672f312e302e301a0e2f697066732f69642f312e302e301a0f2f697066732f6b61642f312e302e30"
        def msg = Hex.decodeHex(hex)
        def protocol = new IdentifyProtocol()

        when:
        def act = protocol.parse(Unpooled.wrappedBuffer(msg))

        then:
        act != null
        act.agentVersion == "parity-polkadot/v0.7.18-fc100e65-x86_64-linux-gnu (unknown)"
        act.listenAddrsCount == 5
        act.protocolsCount == 6
    }

}
