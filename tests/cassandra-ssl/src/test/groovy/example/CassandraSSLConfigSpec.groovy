package example

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.CqlSessionBuilder
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy
import com.datastax.oss.driver.internal.core.loadbalancing.DefaultLoadBalancingPolicy
import io.micronaut.cassandra.CassandraConfiguration
import io.micronaut.cassandra.health.CassandraHealthIndicator
import io.micronaut.context.ApplicationContext
import io.micronaut.context.DefaultApplicationContext
import io.micronaut.context.annotation.Requires
import io.micronaut.context.env.MapPropertySource
import io.micronaut.context.event.BeanCreatedEvent
import io.micronaut.context.event.BeanCreatedEventListener
import io.micronaut.health.HealthStatus
import io.micronaut.http.HttpStatus
import io.micronaut.http.client.HttpClient
import io.micronaut.management.health.indicator.HealthResult
import io.micronaut.runtime.server.EmbeddedServer
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import io.netty.handler.ssl.util.SelfSignedCertificate
import jakarta.inject.Singleton
import org.testcontainers.containers.CassandraContainer
import org.testcontainers.utility.DockerImageName
import reactor.core.publisher.Mono
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification

import java.nio.file.Files
import java.nio.file.Path
import java.security.KeyStore
import java.security.cert.Certificate

@MicronautTest
class CassandraSSLConfigSpec extends Specification {

    static final Path keyStorePath = Files.createTempFile("cassandra-test-key-store", "pkcs12")
    static final Path trustStorePath = Files.createTempFile("cassandra-test-trust-store", "jks")

    @Shared
    @AutoCleanup
    CassandraContainer cassandraContainer = new CassandraContainer<>(DockerImageName.parse("cassandra:latest"))
            .withExposedPorts(9042)

    def setupSpec() {
        setupCert()
        cassandraContainer.start()
    }

    def setupCert() {
        def certificate = new SelfSignedCertificate()

        KeyStore ks = KeyStore.getInstance("PKCS12")
        ks.load(null, null)
        ks.setKeyEntry("key", certificate.key(), "".toCharArray(), new Certificate[]{certificate.cert()})
        try (OutputStream os = Files.newOutputStream(keyStorePath)) {
            ks.store(os, "123456".toCharArray())
        }

        KeyStore ts = KeyStore.getInstance("JKS")
        ts.load(null, null)
        ts.setCertificateEntry("cert", certificate.cert())
        try (OutputStream os = Files.newOutputStream(trustStorePath)) {
            ts.store(os, "123456".toCharArray())
        }
    }

    def sslConfig() {
        [
                // https://docs.datastax.com/en/developer/java-driver/4.17/manual/core/ssl/#driver-configuration
                'cassandra.default.advanced.advanced.ssl-engine-factory.class'                  : 'DefaultSslEngineFactory',
                'cassandra.default.advanced.advanced.ssl-engine-factory.trust-store-path'       : "file://${trustStorePath.toString()}",
                'cassandra.default.advanced.advanced.ssl-engine-factory.trust-store-password'   : '123456',
                'cassandra.default.advanced.advanced.ssl-engine-factory.keystore-path'          : "file://${keyStorePath.toString()}",
                'cassandra.default.advanced.advanced.ssl-engine-factory.key-store-password'     : '123456',
                'cassandra.default.advanced.advanced.ssl-engine-factory.cipher-suites'          : [ "TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA" ]
        ]
    }

    void 'health call succeeds with ssl configured'() {
        when:
        EmbeddedServer embeddedServer = ApplicationContext.run(EmbeddedServer,
                [
                    'spec.name'                                                     : 'CassandraSSLConfigSpec',
                    'cassandra.default.basic.contact-points'                        : ["localhost:$cassandraContainer.firstMappedPort"],
                    'cassandra.default.advanced.metadata.schema.enabled'            : false,
                    'cassandra.default.basic.load-balancing-policy.local-datacenter': 'ociCluster'
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
                    'cassandra.default.basic.contact-points'                        : ["localhost:$cassandraContainer.firstMappedPort"],
                    'cassandra.default.advanced.metadata.schema.enabled'            : false,
                    'cassandra.default.basic.load-balancing-policy.local-datacenter': 'ociCluster'
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
        ((DefaultLoadBalancingPolicy) policies[0]).getLocalDatacenter() == "ociCluster"
        !session.schemaMetadataEnabled
    }

    @Singleton
    @Requires(property='spec.name', value = 'CassandraSSLConfigSpec')
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
