package io.micronaut.cassandra.metrics

import com.datastax.oss.driver.api.core.CqlSession
import io.micrometer.core.instrument.Meter
import io.micrometer.core.instrument.MeterRegistry
import io.micronaut.context.ApplicationContext
import io.micronaut.context.DefaultApplicationContext
import io.micronaut.context.env.MapPropertySource
import io.micronaut.inject.qualifiers.Qualifiers
import org.testcontainers.containers.CassandraContainer
import org.testcontainers.utility.DockerImageName
import spock.lang.Specification

class CassandraMetricsSpec extends Specification {

    void "test Micrometer metrics for cassandra driver"() {
        given:
        CassandraContainer cassandra = new CassandraContainer(DockerImageName.parse('cassandra:latest'))
        cassandra.start()

        ApplicationContext applicationContext = new DefaultApplicationContext("test")
        applicationContext.environment.addPropertySource(MapPropertySource.of('test',
                [
                    'micronaut.metrics.enabled'                                       : true,
                    'cassandra.default.basic.contact-points'                          : ["localhost:$cassandra.firstMappedPort"],
                    'cassandra.default.advanced.metrics.factory.class'                : 'MicrometerMetricsFactory',
                    'cassandra.default.advanced.metrics.session.enabled'              : [ 'connected-nodes', 'cql-requests' ],
                    'cassandra.default.basic.load-balancing-policy.local-datacenter'  : 'datacenter1',
                    'cassandra.secondary.basic.contact-points'                        : ["localhost:$cassandra.firstMappedPort"],
                    'cassandra.secondary.advanced.metrics.factory.class'              : 'MicrometerMetricsFactory',
                    'cassandra.secondary.advanced.metrics.session.enabled'            : [ 'connected-nodes', 'cql-requests' ],
                    'cassandra.secondary.basic.load-balancing-policy.local-datacenter': 'datacenter2',
                ]
        ))
        applicationContext.start()

        when:
        CqlSession defaultCluster = applicationContext.getBean(CqlSession)
        CqlSession secondaryCluster = applicationContext.getBean(CqlSession, Qualifiers.byName("secondary"))
        MeterRegistry meterRegistry = applicationContext.getBean(MeterRegistry)

        then:
        defaultCluster
        secondaryCluster
        meterRegistry

        when:
        List<Meter> auges = meterRegistry.meters.stream().filter {
            it.id.name.contains("connected-nodes")
        }.toList()
        List<Meter> timers = meterRegistry.meters.stream().filter {
            it.id.name.contains("cql-requests")
        }.toList()

        then:
        auges.size() == 2
        timers.size() == 2

        cleanup:
        cassandra.stop()
        applicationContext.close()
    }
}
