package io.emeraldpay.moonbeam.export

import com.fasterxml.jackson.databind.ObjectMapper
import spock.lang.Specification

import java.time.Duration

class FileJsonExportSpec extends Specification {

    def "Parse time limit"() {
        setup:
        def export = new FileJsonExport(File.createTempDir(), "60m", new ObjectMapper(), new FilePostprocessing())
        expect:
        export.parseTimeLimit(strTime) == actTime
        where:
        strTime         | actTime
        "1m"            | Duration.ofMinutes(1)
        "15m"           | Duration.ofMinutes(15)
        "60m"           | Duration.ofMinutes(60)
        "120m"          | Duration.ofMinutes(120)
        "123456789m"    | Duration.ofHours(24)
        "1h"            | Duration.ofHours(1)
        "2h"            | Duration.ofHours(2)
        "12h"           | Duration.ofHours(12)
        "24h"           | Duration.ofHours(24)
        "48h"           | Duration.ofHours(24)
        "1"             | Duration.ofMinutes(1)
        "15"            | Duration.ofMinutes(15)
        "60"            | Duration.ofMinutes(60)
        "120"           | Duration.ofMinutes(120)
    }
}
