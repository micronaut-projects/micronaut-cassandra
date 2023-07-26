package io.micronaut.cassandra.health

import io.micronaut.context.ApplicationContext
import io.micronaut.context.DefaultApplicationContext
import io.micronaut.context.env.MapPropertySource
import io.micronaut.context.env.PropertySource
import io.micronaut.http.HttpStatus
import io.micronaut.http.client.HttpClient
import io.micronaut.runtime.server.EmbeddedServer
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import org.testcontainers.containers.CassandraContainer
import org.testcontainers.utility.DockerImageName
import spock.lang.AutoCleanup
import spock.lang.Issue
import spock.lang.Shared
import spock.lang.Specification

@MicronautTest(rebuildContext = true)
class CassandraHealthIndicatorJacksonSpec extends Specification {

    @Shared @AutoCleanup CassandraContainer cassandraContainer =
            (CassandraContainer) (new CassandraContainer(DockerImageName.parse("cassandra:latest")))
                    .withExposedPorts(9042)

    def setupSpec() {
        cassandraContainer.start()
    }

    void 'cassandra container is running'() {
        expect:
        cassandraContainer.isRunning()
    }

    @Issue("https://github.com/micronaut-projects/micronaut-cassandra/issues/295")
    void 'health call succeeds with jackson.serialization-inclusion set to NON_EMPTY'() {
        given:
        EmbeddedServer embeddedServer = ApplicationContext.run(EmbeddedServer, getConfiguration('NON_EMPTY'), 'test')
        HttpClient client = embeddedServer.getApplicationContext().createBean(HttpClient, embeddedServer.getURL())

        when:
        def response = client.toBlocking().exchange("/health")
        def property = embeddedServer.getEnvironment().getProperty('jackson.serialization-inclusion', String)

        then:
        property.get() == 'NON_EMPTY'
        HttpStatus.OK == response.status()
    }

    @Issue("https://github.com/micronaut-projects/micronaut-cassandra/issues/295")
    void 'health call succeeds with jackson.serialization-inclusion set to ALWAYS'() {
        given:
        EmbeddedServer embeddedServer = ApplicationContext.run(EmbeddedServer, getConfiguration('ALWAYS'), 'test')
        HttpClient client = embeddedServer.getApplicationContext().createBean(HttpClient, embeddedServer.getURL())

        when:
        def response = client.toBlocking().exchange("/health")
        def property = embeddedServer.getEnvironment().getProperty('jackson.serialization-inclusion', String)

        then:
        property.get() == 'ALWAYS'
        HttpStatus.OK == response.status()
    }

    Map<String, Object> getConfiguration(String jacksonInclusion) {
        return [
                'cassandra.default.basic.contact-points': ["localhost:$cassandraContainer.firstMappedPort"],
                'cassandra.default.basic.load-balancing-policy.local-datacenter': 'datacenter1',
                'endpoints.health.cassandra.enabled': true,
                'jackson.serialization-inclusion': jacksonInclusion
        ]
    }
}
