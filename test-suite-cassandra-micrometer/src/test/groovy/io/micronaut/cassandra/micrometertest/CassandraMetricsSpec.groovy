package io.micronaut.cassandra.micrometertest

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
import spock.lang.Requires
import spock.lang.Specification
import org.testcontainers.DockerClientFactory
import java.util.concurrent.TimeUnit

@Property(name = 'spec.name', value = 'CassandraMetricsSpec')
@MicronautTest
@Requires({ DockerClientFactory.instance().isDockerAvailable() })
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
        List<Meter> connectedNodesGauges = meterRegistry.meters.findAll { it.id.name.contains("connected-nodes") }
        List<Meter> cqlRequestsTimers = meterRegistry.meters.findAll { it.id.name.contains("cql-requests") }
        List<Meter> bytesSentMeters = meterRegistry.meters.findAll { it.id.name.contains("bytes-sent") }
        List<Meter> bytesReceivedMeters = meterRegistry.meters.findAll { it.id.name.contains("bytes-received") }

        then:
        connectedNodesGauges.size() == 2
        cqlRequestsTimers.size() == 2
        bytesSentMeters.size() == 2
        bytesReceivedMeters.size() == 2

        when:
        CassandraRepository repository = context.getBean(CassandraRepository)
        def info = repository.getInfo()
        Timer cqlRequests0 = meterRegistry.timer("defaultSession.cql-requests")
        Timer cqlRequests1 = meterRegistry.timer("secondarySession.cql-requests")
        Counter bytesSentS0 = meterRegistry.counter("defaultSession.bytes-sent")
        Counter bytesSentS1 = meterRegistry.counter("secondarySession.bytes-sent")
        Counter bytesReceivedS0 = meterRegistry.counter("defaultSession.bytes-received")
        Counter bytesReceivedS1 = meterRegistry.counter("secondarySession.bytes-received")

        then:
        info.isPresent()
        cqlRequests0.totalTime(TimeUnit.MILLISECONDS) > 0.0
        cqlRequests1.totalTime(TimeUnit.MILLISECONDS) == 0
        bytesSentS0.count() > 0.0
        bytesSentS1.count() > 0.0
        bytesReceivedS0.count() > 0.0
        bytesReceivedS1.count() > 0.0
    }
}
