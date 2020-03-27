package io.emeraldpay.polkadotcrawler.export

import org.springframework.core.env.Environment
import org.springframework.core.env.StandardEnvironment
import spock.lang.Specification

class MysqlExportSpec extends Specification {

    def "Normalize URL"() {
        setup:
        def mysqlExport = new MysqlExport(new StandardEnvironment())
        expect:
        mysqlExport.normalizeUrl(user) == real
        where:
        real                                    | user
        'jdbc:mysql://10.1.0.0:3306/test'       | '10.1.0.0:3306/test'
        'jdbc:mysql://10.1.0.0:3306/test'       | 'mysql://10.1.0.0:3306/test'
        'jdbc:mysql://10.1.0.0:3306/test'       | 'jdbc:mysql://10.1.0.0:3306/test'
    }

}
