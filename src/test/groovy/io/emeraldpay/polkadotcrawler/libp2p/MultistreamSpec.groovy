package io.emeraldpay.polkadotcrawler.libp2p

import io.emeraldpay.polkadotcrawler.DebugCommons
import org.apache.commons.codec.binary.Hex
import reactor.core.publisher.Flux
import reactor.test.StepVerifier
import spock.lang.Specification

import java.nio.ByteBuffer
import java.time.Duration

class MultistreamSpec extends Specification {

    def multistream = new Multistream()

    def "Produces secio header"() {
        when:
        def act = multistream.headerFor("/secio/1.0.0")
        DebugCommons.trace("test", act, false)
        then:
        Hex.encodeHexString(act.array()) == "13" + Hex.encodeHexString("/multistream/1.0.0\n".getBytes()) +
                "0d" + Hex.encodeHexString("/secio/1.0.0\n".getBytes())
    }
    
    def "Read 2 packets"() {
        setup:
        // ./multistream/1.0.0\n
        def p1 = Hex.decodeHex("13" + "2f6d756c746973747265616d2f312e302e300a")
        // ./secio/1.0.0\n....
        def p2 = Hex.decodeHex("0d" + "2f736563696f2f312e302e300a" +
                "00000070" + "0a10790190cd04bb6ba49dfb0b81a1e34ba0122408011220c121ecd16f069680341b54488f345a4905d111a262588517a23badfa33ec9bc91a0b502d3235362c502d333834221a4145532d3132382c4145532d3235362c54776f666973684354522a0d5348413235362c534841353132" +
                "00000085" + "0a41047caf3f33215b08eb11ac7b610e40985d383e39d09ea5cc093d8afde7469da78927882342b45f6706ccacdfa776b7539af929f4b9eecf2441749e64f00cf6f915124065bee0c1b4da7b38227b5e3b6bd9fb79ad8fb2eef78f98126dce32cb3d313c3664207d0086c19dc26c6c721535f0ca50fae13a92eafa04e6ab40ea9f391b0409"
        )

        def source = Flux.fromIterable([p1, p2])
                .map {
                    ByteBuffer.wrap(it)
                }
        when:
        def act = source
                .transform(multistream.readProtocol("/secio/1.0.0", true, null))
                .map { ByteBuffer buf ->
                    return Hex.encodeHexString(buf.array())
                }

        then:
        StepVerifier.create(act)
            .expectNext(
                    "00000070" + "0a10790190cd04bb6ba49dfb0b81a1e34ba0122408011220c121ecd16f069680341b54488f345a4905d111a262588517a23badfa33ec9bc91a0b502d3235362c502d333834221a4145532d3132382c4145532d3235362c54776f666973684354522a0d5348413235362c534841353132" +
                    "00000085" + "0a41047caf3f33215b08eb11ac7b610e40985d383e39d09ea5cc093d8afde7469da78927882342b45f6706ccacdfa776b7539af929f4b9eecf2441749e64f00cf6f915124065bee0c1b4da7b38227b5e3b6bd9fb79ad8fb2eef78f98126dce32cb3d313c3664207d0086c19dc26c6c721535f0ca50fae13a92eafa04e6ab40ea9f391b0409"
            )
            .expectComplete()
            .verify(Duration.ofSeconds(1))

    }

    def "Read 2 header packets, 1 data"() {
        setup:
        // ./multistream/1.0.0\n
        def p1 = Hex.decodeHex("13" + "2f6d756c746973747265616d2f312e302e300a")
        // ./secio/1.0.0\n....
        def p2 = Hex.decodeHex("0d" + "2f736563696f2f312e302e300a")
        def p3 = Hex.decodeHex("00000001ff")

        def multistream = new Multistream()

        def source = Flux.fromIterable([p1, p2, p3])
                .map {
                    ByteBuffer.wrap(it)
                }
        when:
        def act = source
                .transform(multistream.readProtocol("/secio/1.0.0", true, null))
                .map {
                    return Hex.encodeHexString(it.array())
                }

        then:
        StepVerifier.create(act)
                .expectNext(
                        "00000001ff"
                )
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

}
