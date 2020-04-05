package io.emeraldpay.moonbeam.polkadot

import org.apache.commons.codec.binary.Hex
import spock.lang.Specification

class ScaleCodecReader extends Specification {

    def "Read uint32"() {
        setup:
        expect:
        new ScaleCodec.Reader(Hex.decodeHex(hex)).getUint32() == value
        where:
        hex         | value
        "ffffff00"  | 16777215
        "00060000"  | 1536
        "00030000"  | 768
        "7d010000"  | 381
    }

    def "Read uint16"() {
        setup:
        expect:
        new ScaleCodec.Reader(Hex.decodeHex(hex)).getUint16() == value
        where:
        hex         | value
        "2a00"      | 42
    }

    def "Read byte"() {
        setup:
        expect:
        new ScaleCodec.Reader(Hex.decodeHex(hex)).getByte() == value.byteValue()
        where:
        hex     | value
        "00"    | 0
        "45"    | 69
    }

    def "Read compact int"() {
        expect:
        new ScaleCodec.Reader(Hex.decodeHex(hex)).getCompactInt() == value
        where:
        hex         | value
        "00"        | 0
        "04"        | 1
        "a8"        | 42
        "fc"        | 63
        "0101"      | 64
        "1501"      | 69
        "fdff"      | 16383
        "02000100"  | 16384
        "feffffff"  | 1073741823
//        "0300000040"| 1073741824
    }

    def "Read byte array"() {
        expect:
        Hex.encodeHexString(new ScaleCodec.Reader(Hex.decodeHex(hex)).getByteArray()) == value
        where:
        hex         | value
        "00"        | ""
        "0401"      | "01"
    }

    def "Read fixed byte array"() {
        expect:
        Hex.encodeHexString(new ScaleCodec.Reader(Hex.decodeHex(hex)).getByteArray(32)) == hex
        where:
        hex << ["bb931fd17f85fb26e8209eb7af5747258163df29a7dd8f87fa7617963fcfa1aa"]
    }

    def "Parse status message"() {
        setup:
        def msg = Hex.decodeHex("000600000003000000017d010000bb931fd17f85fb26e8209eb7af5747258163df29a7dd8f87fa7617963fcfa1aab0a8d493285c2df73290dfb7e61f870f17b41801197a149ca93654499ea3dafe0400")
        def rdr = new ScaleCodec.Reader(msg)

        when:
        def act = rdr.getUint32() // version
        then:
        act == 1536

        when:
        act = rdr.getUint32() // min version
        then:
        act == 768

        when:
        act = rdr.getByte() // roles
        then:
        act == 0.byteValue()

        when:
        act = rdr.getByte() // ?

        act = rdr.getUint32() // height
        then:
        act == 381

        when:
        act = rdr.getByteArray(32) // best hash
        then:
        Hex.encodeHexString(act) == "bb931fd17f85fb26e8209eb7af5747258163df29a7dd8f87fa7617963fcfa1aa"

        when:
        act = rdr.getByteArray(32) // genesis hash
        then:
        Hex.encodeHexString(act) == "b0a8d493285c2df73290dfb7e61f870f17b41801197a149ca93654499ea3dafe"

    }
}
