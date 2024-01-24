package io.micronaut.cassandra.ssltest

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.CqlSessionBuilder
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy
import io.micronaut.cassandra.CassandraConfiguration
import io.micronaut.cassandra.health.CassandraHealthIndicator
import io.micronaut.context.ApplicationContext
import io.micronaut.context.DefaultApplicationContext
import io.micronaut.context.annotation.Requires
import io.micronaut.context.env.MapPropertySource
import io.micronaut.context.event.BeanCreatedEvent
import io.micronaut.context.event.BeanCreatedEventListener
import io.micronaut.context.exceptions.BeanInstantiationException
import io.micronaut.health.HealthStatus
import io.micronaut.http.HttpStatus
import io.micronaut.http.client.HttpClient
import io.micronaut.management.health.indicator.HealthResult
import io.micronaut.runtime.server.EmbeddedServer
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import jakarta.inject.Singleton
import org.testcontainers.containers.BindMode
import org.testcontainers.containers.CassandraContainer
import org.testcontainers.utility.DockerImageName
import reactor.core.publisher.Mono
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification

@MicronautTest
class CassandraSSLConfigSpec extends Specification {

    @Shared
    @AutoCleanup
    CassandraContainer cassandraContainer = new CassandraContainer<>(DockerImageName.parse("cassandra:latest"))
            .withClasspathResourceMapping("/certs/keystore.shared", "/opt/cassandra/conf/certs/cassandra.keystore", BindMode.READ_ONLY)
            .withClasspathResourceMapping("/certs/truststore.shared", "/opt/cassandra/conf/certs/cassandra.truststore", BindMode.READ_ONLY)
            .withClasspathResourceMapping("/ssl-cassandra.yaml", "/etc/cassandra/cassandra.yaml", BindMode.READ_ONLY)

    def setupSpec() {
        cassandraContainer.start()
    }

    def sslConfig() {
        [
                // https://docs.datastax.com/en/developer/java-driver/4.17/manual/core/ssl/#driver-configuration
                'cassandra.default.advanced.ssl-engine-factory.class'               : 'com.datastax.oss.driver.internal.core.ssl.DefaultSslEngineFactory',
                'cassandra.default.advanced.ssl-engine-factory.truststore-path'     : new File(CassandraSSLConfigSpec.getResource("/certs/truststore.shared").file).absolutePath,
                'cassandra.default.advanced.ssl-engine-factory.truststore-password' : 'cassandra',
                'cassandra.default.advanced.ssl-engine-factory.hostname-validation' : 'false',
        ]
    }

    void 'health call succeeds with ssl configured'() {
        when:
        println sslConfig()
        EmbeddedServer embeddedServer = ApplicationContext.run(EmbeddedServer,
                [
                        'spec.name'                                                     : 'CassandraSSLConfigSpec',
                        'cassandra.default.basic.session-name'                          : 'defaultSession',
                        'cassandra.default.basic.contact-points'                        : ["localhost:$cassandraContainer.firstMappedPort"],
                        'cassandra.default.advanced.metadata.schema.enabled'            : false,
                        'cassandra.default.basic.load-balancing-policy.local-datacenter': 'datacenter1'
                ] + sslConfig(),
                "test")
        HttpClient client = embeddedServer.getApplicationContext().createBean(HttpClient, embeddedServer.getURL())

        def response = client.toBlocking().exchange("/health", CassandraHealthIndicator)
        CassandraHealthIndicator healthIndicator = embeddedServer.getApplicationContext().getBean(CassandraHealthIndicator)
        HealthResult result = Mono.from(healthIndicator.result).block()

        then:
        response.status() == HttpStatus.OK
        result.status == HealthStatus.UP
    }

    void 'test single cluster connection with ssl configured'() {
        given:
        ApplicationContext applicationContext = new DefaultApplicationContext("test")
        applicationContext.environment.addPropertySource(MapPropertySource.of(
                'test',
                [
                        'spec.name'                                                     : 'CassandraSSLConfigSpec',
                        'cassandra.default.basic.session-name'                          : 'defaultSession',
                        'cassandra.default.basic.contact-points'                        : ["localhost:$cassandraContainer.firstMappedPort"],
                        'cassandra.default.advanced.metadata.schema.enabled'            : false,
                        'cassandra.default.basic.load-balancing-policy.local-datacenter': 'datacenter1'
                ] + sslConfig()
        ))
        applicationContext.start()


        expect:
        !applicationContext.getBean(CqlSessionBuilderListener).invoked
        applicationContext.containsBean(CassandraConfiguration)
        applicationContext.containsBean(CqlSession)

        when:
        CqlSession session = applicationContext.getBean(CqlSession)
        applicationContext.getBean(CqlSessionBuilderListener).invoked
        Collection<LoadBalancingPolicy> policies = session.getContext().loadBalancingPolicies.values()

        then:
        policies[0].localDatacenter == 'datacenter1'
        !session.schemaMetadataEnabled

        when:
        CqlSession defaultCluster = applicationContext.getBean(CqlSession)
        CassandraRepository repository = applicationContext.getBean(CassandraRepository)
        def info = repository.getInfo()

        then:
        defaultCluster
        info.isPresent()
    }

    void 'test single cluster connection with ssl un-configured'() {
        given:
        ApplicationContext applicationContext = new DefaultApplicationContext("test")
        applicationContext.environment.addPropertySource(MapPropertySource.of(
                'test',
                [
                        'spec.name'                                                     : 'CassandraSSLConfigSpec',
                        'cassandra.default.basic.session-name'                          : 'defaultSession',
                        'cassandra.default.basic.contact-points'                        : ["localhost:$cassandraContainer.firstMappedPort"],
                        'cassandra.default.advanced.metadata.schema.enabled'            : false,
                        'cassandra.default.basic.load-balancing-policy.local-datacenter': 'datacenter1'
                ] // No SSL config
        ))
        applicationContext.start()


        expect:
        !applicationContext.getBean(CqlSessionBuilderListener).invoked
        applicationContext.containsBean(CassandraConfiguration)
        applicationContext.containsBean(CqlSession)

        when:
        applicationContext.getBean(CqlSession)

        then:
        thrown(BeanInstantiationException)
    }

    @Singleton
    @Requires(property = 'spec.name', value = 'CassandraSSLConfigSpec')
    static class CqlSessionBuilderListener implements BeanCreatedEventListener<CqlSessionBuilder> {
        boolean invoked = false

        @Override
        CqlSessionBuilder onCreated(BeanCreatedEvent<CqlSessionBuilder> event) {
            def builder = event.getBean()
            invoked = builder != null
            return builder
        }
    }
}
