package io.micronaut.cassandra.metrics

import com.datastax.oss.driver.api.core.CqlSession
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Meter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import io.micronaut.context.ApplicationContext
import io.micronaut.inject.qualifiers.Qualifiers
import org.testcontainers.containers.CassandraContainer
import org.testcontainers.utility.DockerImageName
import spock.lang.Issue
import spock.lang.Specification
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables

class CassandraMetricsSpec extends Specification {

    void "test Micrometer metrics for cassandra driver"() {
        given:
        CassandraContainer cassandra = new CassandraContainer(DockerImageName.parse('cassandra:latest'))
        cassandra.start()

        ApplicationContext applicationContext = ApplicationContext.run(
                'micronaut.metrics.enabled': true,
                'cassandra.default.basic.contact-points': ["localhost:$cassandra.firstMappedPort"],
                'cassandra.default.basic.session-name': 'defaultSession',
                'cassandra.default.advanced.metrics.factory.class': 'MicrometerMetricsFactory',
                'cassandra.default.advanced.metrics.session.enabled': ['connected-nodes', 'cql-requests'],
                'cassandra.default.basic.load-balancing-policy.local-datacenter': 'datacenter1',
                'cassandra.secondary.basic.contact-points': ["localhost:$cassandra.firstMappedPort"],
                'cassandra.secondary.basic.session-name': 'secondarySession',
                'cassandra.secondary.advanced.metrics.factory.class': 'MicrometerMetricsFactory',
                'cassandra.secondary.advanced.metrics.session.enabled': ['connected-nodes', 'cql-requests'],
                'cassandra.secondary.basic.load-balancing-policy.local-datacenter': 'datacenter2',
                'test'
        )

        when:
        CqlSession defaultCluster = applicationContext.getBean(CqlSession)
        CqlSession secondaryCluster = applicationContext.getBean(CqlSession, Qualifiers.byName("secondary"))
        MeterRegistry meterRegistry = applicationContext.getBean(MeterRegistry)

        then:
        defaultCluster
        secondaryCluster
        meterRegistry

        when:
        List<Meter> gauges = meterRegistry.meters.findAll { it.id.name.contains("connected-nodes") }
        List<Meter> timers = meterRegistry.meters.findAll { it.id.name.contains("cql-requests") }

        then:
        gauges.size() == 2
        timers.size() == 2

        cleanup:
        cassandra.stop()
        applicationContext.close()
    }

    @Issue("https://github.com/micronaut-projects/micronaut-cassandra/issues/240")
    void "test metrics with overriding env vars"() {
        given:
        // override: cassandra.default.basic.session-name=defaultSession
        def env = new EnvironmentVariables("CASSANDRA_DEFAULT_BASIC_SESSION-NAME", "envSession")
        env.setup()

        CassandraContainer cassandra = new CassandraContainer(DockerImageName.parse('cassandra:latest'))
        cassandra.start()

        ApplicationContext applicationContext = ApplicationContext.run(
                'micronaut.metrics.enabled': true,
                'cassandra.default.basic.contact-points': ["localhost:$cassandra.firstMappedPort"],
                'cassandra.default.basic.session-name': 'defaultSession',
                'cassandra.default.advanced.metrics.factory.class': 'MicrometerMetricsFactory',
                'cassandra.default.advanced.metrics.session.enabled': ['connected-nodes', 'cql-requests'],
                'cassandra.default.basic.load-balancing-policy.local-datacenter': 'datacenter1',
                'test'
        )

        when:
        CqlSession defaultCluster = applicationContext.getBean(CqlSession)
        MeterRegistry meterRegistry = applicationContext.getBean(MeterRegistry)

        then:
        defaultCluster
        meterRegistry

        when:
        Timer cqlRequests = meterRegistry.timer("envSession.cql-requests")
        Counter bytesSent = meterRegistry.counter("envSession.bytes-sent")
        Counter bytesReceived = meterRegistry.counter("envSession.bytes-received")

        then:
        cqlRequests
        bytesSent
        bytesReceived

        cleanup:
        env.teardown()
        cassandra.stop()
        applicationContext.close()
    }
}
