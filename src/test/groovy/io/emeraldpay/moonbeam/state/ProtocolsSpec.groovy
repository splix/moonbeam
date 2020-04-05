package io.emeraldpay.moonbeam.state

import spock.lang.Specification

class ProtocolsSpec extends Specification {

    def "Accept kusama ls"() {
        setup:
        def protocols = [
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
        when:
        def act = new Protocols(protocols)
        then:
        act.versions.size() == 6
        with(act.versions[0]) {
            id == "/substrate/ksmcc3"
            versions == ["6", "5", "4", "3"]
        }
        with(act.versions[1]) {
            id == "/ipfs/ping"
            versions == ["1.0.0"]
        }
        with(act.versions[2]) {
            id == "/ipfs/id"
            versions == ["1.0.0"]
        }
        with(act.versions[3]) {
            id == "/ipfs/kad"
            versions == ["1.0.0"]
        }
        with(act.versions[4]) {
            id == "/ksmcc3/sync"
            versions == ["1"]
        }
        with(act.versions[5]) {
            id == "/ksmcc3/light"
            versions == ["1"]
        }

    }
}
