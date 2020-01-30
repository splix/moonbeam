package io.emeraldpay.polkadotcrawler.libp2p

import io.emeraldpay.polkadotcrawler.DebugCommons
import io.netty.buffer.Unpooled
import org.bouncycastle.util.encoders.Hex
import spock.lang.Specification

class MplexSpec extends Specification {

    def "Parse single packets"() {
        setup:
        // header + ./multistream/1.0.0\n
        def p1 = Hex.decode("c93e14132f6d756c746973747265616d2f312e302e300a")

        def mplex = new Mplex()
        when:
        def act = mplex.parse(Unpooled.wrappedBuffer(p1))

        then:
        act.size() == 1
        with(act[0]) {
            header.flag == Mplex.Flag.MessageReceiver
            header.id == 1001
            DebugCommons.toHex(data) == "132f6d756c746973747265616d2f312e302e300a"
        }
    }

    def "Parse two packets"() {
        setup:
        // header + ./ipfs/id.. + header + data
        def p2 = Hex.decode("c93e100f2f697066732f69642f312e302e300a c93eb802b6022a0e2f7375627374726174652f312e30323b7061726974792d706f6c6b61646f742f76302e372e31382d66633130306536352d7838365f36342d6c696e75782d676e752028756e6b6e6f776e290a2408011220a2aa2a4251027b5ef2d1c79700aa20eebf47c493df4ebfd7c1aa71f202bc28c9121d36187032702e6363332d322e6b7573616d612e6e6574776f726b06759412080422d90d3b0675941208040a0a0a010675941208047f0000010675941208040a0001730675942208040a0001880663641a132f7375627374726174652f6b736d6363332f351a132f7375627374726174652f6b736d6363332f341a132f7375627374726174652f6b736d6363332f331a102f697066732f70696e672f312e302e301a0e2f697066732f69642f312e302e301a0f2f697066732f6b61642f312e302e30")

        def mplex = new Mplex()
        when:
        def act = mplex.parse(Unpooled.wrappedBuffer(p2))

        then:
        act.size() == 2
        with(act[0]) {
            header.flag == Mplex.Flag.MessageReceiver
            header.id == 1001
            DebugCommons.toHex(data) == "0f2f697066732f69642f312e302e300a"
        }
        with(act[1]) {
            header.flag == Mplex.Flag.MessageReceiver
            header.id == 1001
            DebugCommons.toHex(data) == "b6022a0e2f7375627374726174652f312e30323b7061726974792d706f6c6b61646f742f76302e372e31382d66633130306536352d7838365f36342d6c696e75782d676e752028756e6b6e6f776e290a2408011220a2aa2a4251027b5ef2d1c79700aa20eebf47c493df4ebfd7c1aa71f202bc28c9121d36187032702e6363332d322e6b7573616d612e6e6574776f726b06759412080422d90d3b0675941208040a0a0a010675941208047f0000010675941208040a0001730675942208040a0001880663641a132f7375627374726174652f6b736d6363332f351a132f7375627374726174652f6b736d6363332f341a132f7375627374726174652f6b736d6363332f331a102f697066732f70696e672f312e302e301a0e2f697066732f69642f312e302e301a0f2f697066732f6b61642f312e302e30"
        }
    }
}
