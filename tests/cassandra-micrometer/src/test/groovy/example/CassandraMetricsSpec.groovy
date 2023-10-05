/*
 * Copyright 2017-2023 original authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package example

import com.datastax.oss.driver.api.core.CqlSession
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Meter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import io.micronaut.context.BeanContext
import io.micronaut.context.annotation.Property
import io.micronaut.inject.qualifiers.Qualifiers
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import jakarta.inject.Inject
import spock.lang.Specification

import java.util.concurrent.TimeUnit

import static org.junit.jupiter.api.Assertions.assertTrue

@Property(name = 'spec.name', value = 'CassandraMetricsSpec')
@MicronautTest
class CassandraMetricsSpec extends Specification {
    @Inject
    BeanContext context

    void "test metrics"() {
        when:
        CqlSession defaultCluster = context.getBean(CqlSession)
        CqlSession secondaryCluster = context.getBean(CqlSession, Qualifiers.byName("secondary"))
        MeterRegistry meterRegistry = context.getBean(MeterRegistry)

        then:
        defaultCluster
        secondaryCluster
        meterRegistry

        when:
        List<Meter> connectedNodesGauges = meterRegistry.meters.stream().filter {
            it.id.name.contains("connected-nodes")
        }.toList()
        List<Meter> cqlRequestsTimers = meterRegistry.meters.stream().filter {
            it.id.name.contains("cql-requests")
        }.toList()
        List<Meter> bytesSentMeters = meterRegistry.meters.stream().filter {
            it.id.name.contains("bytes-sent")
        }.toList()
        List<Meter> bytesReceivedMeters = meterRegistry.meters.stream().filter {
            it.id.name.contains("bytes-received")
        }.toList()

        then:
        connectedNodesGauges.size() == 2
        cqlRequestsTimers.size() == 2
        bytesSentMeters.size() == 2
        bytesReceivedMeters.size() == 2

        when:
        CassandraRepository repository = context.getBean(CassandraRepository)
        repository.getInfo()
        Timer cqlRequests0 = meterRegistry.timer("s0.cql-requests")
        Timer cqlRequests1 = meterRegistry.timer("s1.cql-requests")
        Counter bytesSentS0 = meterRegistry.counter("s0.bytes-sent")
        Counter bytesSentS1 = meterRegistry.counter("s1.bytes-sent")
        Counter bytesReceivedS0 = meterRegistry.counter("s0.bytes-received")
        Counter bytesReceivedS1 = meterRegistry.counter("s1.bytes-received")

        then:
        cqlRequests0.totalTime(TimeUnit.MILLISECONDS) > 0.0
        cqlRequests1.totalTime(TimeUnit.MILLISECONDS) == 0
        bytesSentS0.count() > 0.0
        bytesSentS1.count() > 0.0
        bytesReceivedS0.count() > 0.0
        bytesReceivedS1.count() > 0.0
    }
}
