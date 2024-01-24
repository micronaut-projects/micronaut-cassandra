package io.micronaut.cassandra.jacksontest

import com.fasterxml.jackson.annotation.JsonInclude
import io.micronaut.cassandra.health.CassandraHealthIndicator
import io.micronaut.context.ApplicationContext
import io.micronaut.health.HealthStatus
import io.micronaut.http.HttpStatus
import io.micronaut.http.client.HttpClient
import io.micronaut.management.health.indicator.HealthResult
import io.micronaut.runtime.server.EmbeddedServer
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import org.testcontainers.containers.CassandraContainer
import org.testcontainers.utility.DockerImageName
import reactor.core.publisher.Mono
import spock.lang.AutoCleanup
import spock.lang.Issue
import spock.lang.Shared
import spock.lang.Specification

@MicronautTest
class CassandraHealthEndpointJacksonSpec extends Specification {

    @Shared @AutoCleanup CassandraContainer cassandraContainer =
            new CassandraContainer<>(DockerImageName.parse("cassandra:latest")).withExposedPorts(9042)

    def setupSpec() {
        cassandraContainer.start()
    }

    // fails when using jackson-databind if CassandraHealthIndicator isn't @Introspected
    @Issue("https://github.com/micronaut-projects/micronaut-cassandra/issues/295")
    void 'health call succeeds with jackson.serialization-inclusion = #inclusion'() {
        when:
        EmbeddedServer embeddedServer = ApplicationContext.run(EmbeddedServer, [
                'cassandra.default.basic.contact-points': ["localhost:$cassandraContainer.firstMappedPort"],
                'cassandra.default.basic.load-balancing-policy.local-datacenter': 'datacenter1',
                'jackson.serialization-inclusion': inclusion], "test")
        HttpClient client = embeddedServer.getApplicationContext().createBean(HttpClient, embeddedServer.getURL())

        def response = client.toBlocking().exchange("/health", CassandraHealthIndicator)
        CassandraHealthIndicator healthIndicator = embeddedServer.getApplicationContext().getBean(CassandraHealthIndicator)
        HealthResult result = Mono.from(healthIndicator.result).block()

        then:
        response.status() == HttpStatus.OK
        result
        result.status == HealthStatus.UP

        where:
        inclusion << JsonInclude.Include.values()
    }
}
