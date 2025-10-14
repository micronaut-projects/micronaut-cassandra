package io.micronaut.cassandra.metrics

import com.datastax.oss.driver.api.core.CqlSession
import io.micrometer.core.instrument.Meter
import io.micrometer.core.instrument.MeterRegistry
import io.micronaut.context.ApplicationContext
import io.micronaut.core.value.PropertyResolver
import io.micronaut.inject.qualifiers.Qualifiers
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.CassandraContainer
import org.testcontainers.utility.DockerImageName
import spock.lang.Issue
import spock.lang.Requires
import spock.lang.Specification
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables

@Requires({ DockerClientFactory.instance().isDockerAvailable() })
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
    void "test metrics with overriding #configStyle with env vars"() {
        given:
        CassandraContainer cassandra = new CassandraContainer(DockerImageName.parse('cassandra:latest'))
        cassandra.start()

        // override: cassandra.default.basic.session-name=defaultSession
        def env = new EnvironmentVariables(
                "CASSANDRA_DEFAULT_BASIC_SESSION_NAME", "envSession",
                "CASSANDRA_PORT", "${cassandra.firstMappedPort}"
        )
        env.setup()

        ApplicationContext applicationContext = ApplicationContext.run("env$configStyle")

        when:
        CqlSession defaultCluster = applicationContext.getBean(CqlSession)
        PropertyResolver resolver = applicationContext.getBean(PropertyResolver)
        MeterRegistry meterRegistry = applicationContext.getBean(MeterRegistry)

        then:
        resolver.getRequiredProperty("configuration", String) == configStyle
        defaultCluster
        meterRegistry

        and:
        meterRegistry.meters.id.name.findAll { it.contains("envSession") } ==~ ['envSession.connected-nodes', 'envSession.cql-requests']

        cleanup:
        env.teardown()
        cassandra.stop()
        applicationContext.close()

        where:
        configStyle << ['props', 'yaml']
    }
}
