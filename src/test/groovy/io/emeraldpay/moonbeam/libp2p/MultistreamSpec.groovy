package io.emeraldpay.moonbeam.libp2p

import io.emeraldpay.moonbeam.DebugCommons
import org.apache.commons.codec.binary.Hex
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.extra.processor.TopicProcessor
import reactor.test.StepVerifier
import spock.lang.Specification

import java.nio.ByteBuffer
import java.time.Duration
import java.util.function.Function

class MultistreamSpec extends Specification {

    def multistream = new Multistream()

    def "Produces secio header"() {
        when:
        def act = multistream.multistreamHeader("/secio/1.0.0")
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

    def "Extracts null protocol from not fully filled"() {
        expect:
        def act = multistream.splitProtocol(ByteBuffer.wrap(
                Hex.decodeHex(inputs)
        ))
        act.isEmpty()
        where:
        inputs << [
                "",
                "13" + "2f6d756c7469737472", //cut of mulstistream
        ]
    }

    def "Extracts protocol"() {
        when:
        def act = multistream.splitProtocol(ByteBuffer.wrap(
                Hex.decodeHex("13" + "2f6d756c746973747265616d2f312e302e300a" +
                        "0d" + "2f736563696f2f312e302e300a")
        ))
        then:
        act == ["/multistream/1.0.0", "/secio/1.0.0"]
    }

    def "Extracts single protocol"() {
        when:
        def act = multistream.splitProtocol(ByteBuffer.wrap(
                Hex.decodeHex("0d" + "2f736563696f2f312e302e300a")
        ))
        then:
        act == ["/secio/1.0.0"]
    }

    def "Ignores tail when extracts protocol"() {
        when:
        def act = multistream.splitProtocol(ByteBuffer.wrap(
                Hex.decodeHex("13" + "2f6d756c746973747265616d2f312e302e300a" +
                        "0d" + "2f736563696f2f312e302e300a" + "ff1234")
        ))
        then:
        act == ["/multistream/1.0.0", "/secio/1.0.0"]
    }

    def "Negotiate when first is not supported"() {
        setup:
        TopicProcessor<String> remote = TopicProcessor.create()
        Flux<ByteBuffer> inbound = Flux.from(remote).map { ByteBuffer.wrap(Hex.decodeHex(it)) }
        Function<Flux<ByteBuffer>, Flux<ByteBuffer>> handler = {
            Mono.just(ByteBuffer.wrap(Hex.decodeHex("796573")))
        }
        when:
        def act = multistream.negotiate(
                inbound,
                "/secio/1.0.0",
                handler
        ).map {
            Hex.encodeHexString(it.array())
        }

        remote.onNext(
                "13" + "2f6d756c746973747265616d2f312e302e300a" +
                "0d" + "2f6e6f7365632f312e302e300a" //nosec/1.0.0
        )
        then:
        StepVerifier.create(act)
            .expectNext("132f6d756c746973747265616d2f312e302e300a").as("Handshake")
            .expectNext("036e610a").as("Send NA")
            .then {
                remote.onNext(
                        "0d" + "2f736563696f2f312e302e300a" // secio/1.0.0
                )
            }.as("Remote proposes Secio")
            .expectNext("0d2f736563696f2f312e302e300a").as("Confirmation")
            .expectNext("796573").as("The handler data")
            .expectComplete()
            .verify(Duration.ofSeconds(1))
    }

    def "Negotiate when first is supported, we start"() {
        setup:
        TopicProcessor<String> remote = TopicProcessor.create()
        Flux<ByteBuffer> inbound = Flux.from(remote).map { ByteBuffer.wrap(Hex.decodeHex(it)) }
        Function<Flux<ByteBuffer>, Flux<ByteBuffer>> handler = {
            Mono.just(ByteBuffer.wrap(Hex.decodeHex("796573")))
        }
        when:
        def act = multistream.negotiate(
                inbound,
                "/secio/1.0.0",
                handler
        ).map {
            Hex.encodeHexString(it.array())
        }

        then:
        StepVerifier.create(act)
                .expectNext("132f6d756c746973747265616d2f312e302e300a").as("Handshake")
                .then {
                    remote.onNext(
                            "13" + "2f6d756c746973747265616d2f312e302e300a" +
                                    "0d" + "2f736563696f2f312e302e300a" //secio/1.0.0
                    )
                }
                .expectNext("0d2f736563696f2f312e302e300a").as("Confirmation")
                .expectNext("796573").as("The handler data")
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "Negotiate when first is supported, remote start"() {
        setup:
        TopicProcessor<String> remote = TopicProcessor.create()
        Flux<ByteBuffer> inbound = Flux.from(remote).map { ByteBuffer.wrap(Hex.decodeHex(it)) }
        Function<Flux<ByteBuffer>, Flux<ByteBuffer>> handler = {
            Mono.just(ByteBuffer.wrap(Hex.decodeHex("796573")))
        }
        when:
        def act = multistream.negotiate(
                inbound,
                "/secio/1.0.0",
                handler
        ).map {
            Hex.encodeHexString(it.array())
        }

        remote.onNext(
                "13" + "2f6d756c746973747265616d2f312e302e300a" +
                        "0d" + "2f736563696f2f312e302e300a" //secio/1.0.0
        )

        then:
        StepVerifier.create(act)
                .expectNext("132f6d756c746973747265616d2f312e302e300a").as("Handshake")
                .expectNext("0d2f736563696f2f312e302e300a").as("Confirmation")
                .expectNext("796573").as("The handler data")
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "Propose - accepted"() {
        setup:
        TopicProcessor<String> remote = TopicProcessor.create()
        Flux<ByteBuffer> inbound = Flux.from(remote).map { ByteBuffer.wrap(Hex.decodeHex(it)) }
        Function<Flux<ByteBuffer>, Flux<ByteBuffer>> handler = {
            Mono.just(ByteBuffer.wrap(Hex.decodeHex("796573")))
        }
        when:
        def act = multistream.propose(
                inbound,
                "/secio/1.0.0",
                handler
        ).map {
            Hex.encodeHexString(it.array())
        }

        then:
        StepVerifier.create(act)
                .expectNext("132f6d756c746973747265616d2f312e302e300a" + "0d2f736563696f2f312e302e300a").as("Proposal")
                .then {
                    remote.onNext(
                            "13" + "2f6d756c746973747265616d2f312e302e300a" + // mulstistream/1.0.0
                            "0d" + "2f736563696f2f312e302e300a" // secio/1.0.0
                    )
                }.as("Remote proposes Secio too")
                .expectNext("796573").as("The handler data")
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }
    def "Propose - accepted in two messages"() {
        setup:
        TopicProcessor<String> remote = TopicProcessor.create()
        Flux<ByteBuffer> inbound = Flux.from(remote).map { ByteBuffer.wrap(Hex.decodeHex(it)) }
        Function<Flux<ByteBuffer>, Flux<ByteBuffer>> handler = {
            Mono.just(ByteBuffer.wrap(Hex.decodeHex("796573")))
        }
        when:
        def act = multistream.propose(
                inbound,
                "/secio/1.0.0",
                handler
        ).map {
            Hex.encodeHexString(it.array())
        }

        then:
        StepVerifier.create(act)
                .expectNext("132f6d756c746973747265616d2f312e302e300a" + "0d2f736563696f2f312e302e300a").as("Proposal")
                .then {
                    remote.onNext(
                            "13" + "2f6d756c746973747265616d2f312e302e300a" // mulstistream/1.0.0
                    )
                    remote.onNext(
                            "0d" + "2f736563696f2f312e302e300a" // secio/1.0.0
                    )
                }.as("Remote proposes Secio too")
                .expectNext("796573").as("The handler data")
                .expectComplete()
                .verify(Duration.ofSeconds(1))
    }

    def "Parse list"() {
        setup:
        def data = "09142f7375627374726174652f6b736d6363332f360a142f7375627374726174652f6b736d6363332f350a142f7375627374726174652f6b736d6363332f340a142f7375627374726174652f6b736d6363332f330a112f697066732f70696e672f312e302e300a0f2f697066732f69642f312e302e300a102f697066732f6b61642f312e302e300a0f2f6b736d6363332f73796e632f310a102f6b736d6363332f6c696768742f310a"

        when:
        def act = multistream.parseList(ByteBuffer.wrap(Hex.decodeHex(data)))

        then:

        act == [
                "/substrate/ksmcc3/6",
                "/substrate/ksmcc3/5",
                "/substrate/ksmcc3/4",
                "/substrate/ksmcc3/3",
                "/ipfs/ping/1.0.0",
                "/ipfs/id/1.0.0",
                "/ipfs/kad/1.0.0",
                "/ksmcc3/sync/1",
                "/ksmcc3/light/1"
        ]
    }
}
