/*
 * Copyright 2017-2018 original authors
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
package io.micronaut.cassandra.health

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.CqlSessionBuilder
import io.micronaut.cassandra.CassandraConfiguration
import io.micronaut.context.ApplicationContext
import io.micronaut.context.DefaultApplicationContext
import io.micronaut.context.env.MapPropertySource
import io.micronaut.context.event.BeanCreatedEvent
import io.micronaut.context.event.BeanCreatedEventListener
import io.micronaut.context.exceptions.NoSuchBeanException
import io.micronaut.health.HealthStatus
import io.micronaut.inject.qualifiers.Qualifiers
import io.micronaut.management.health.indicator.HealthResult
import jakarta.inject.Singleton
import org.testcontainers.containers.CassandraContainer
import org.testcontainers.utility.DockerImageName
import reactor.core.publisher.Mono
import spock.lang.Specification

/**
 * @author Ilkin Ashrafli
 * @since 2.2.0
 */
class CassandraHealthIndicatorSpec extends Specification {

    void "test cassandra health indicator"() {
        given:
        CassandraContainer cassandraContainer = new CassandraContainer(DockerImageName.parse('cassandra:latest'))
        cassandraContainer.start()

        // tag::single[]
        ApplicationContext applicationContext = new DefaultApplicationContext('test')
        applicationContext.environment.addPropertySource(MapPropertySource.of(
                'test',
                ['cassandra.default.basic.contact-points'                        : ["localhost:$cassandraContainer.firstMappedPort"],
                 'cassandra.default.advanced.metadata.schema.enabled'            : false,
                 'cassandra.default.basic.load-balancing-policy.local-datacenter': 'datacenter1']
        ))
        applicationContext.start()
        // end::single[]

        expect:
        !applicationContext.getBean(CqlSessionBuilderListener).invoked
        applicationContext.containsBean(CassandraConfiguration)
        applicationContext.containsBean(CqlSession)

        when:
        CassandraHealthIndicator healthIndicator = applicationContext.getBean(CassandraHealthIndicator)
        applicationContext.getBean(CqlSessionBuilderListener).invoked
        HealthResult result = Mono.from(healthIndicator.result).block()
        Map<String, Map<String, Object>> detailsMap = (Map<String, Map<String, Object>>) result.details

        then:
        result.status == HealthStatus.UP
        detailsMap.size() == 1
        detailsMap.values().each { {
            it['nodes_count']
            it['nodes_state']
            it['session'] == 'OPEN'
        }}

        when:
        cassandraContainer.stop()
        result = Mono.from(healthIndicator.result).block()

        then:
        result.status == HealthStatus.DOWN

        cleanup:
        applicationContext.close()
    }

    void "test cassandra health indicator with multiple configs"() {
        given:
        CassandraContainer cassandraContainer = new CassandraContainer(DockerImageName.parse('cassandra:latest'))
        cassandraContainer.start()

        ApplicationContext applicationContext = new DefaultApplicationContext('test')
        applicationContext.environment.addPropertySource(MapPropertySource.of(
                'test',
                ['cassandra.default.basic.contact-points'                          : ["localhost:$cassandraContainer.firstMappedPort"],
                 'cassandra.default.advanced.metadata.schema.enabled'              : false,
                 'cassandra.default.basic.load-balancing-policy.local-datacenter'  : 'datacenter1',
                 'cassandra.secondary.basic.contact-points'                        : ["localhost:$cassandraContainer.firstMappedPort"],
                 'cassandra.secondary.advanced.metadata.schema.enabled'            : false,
                 'cassandra.secondary.basic.load-balancing-policy.local-datacenter': 'datacenter2',
                 'cassandra.tertiary.basic.contact-points'                        : ["localhost:$cassandraContainer.firstMappedPort"],
                 'cassandra.tertiary.advanced.metadata.schema.enabled'            : false,
                 'cassandra.tertiary.basic.load-balancing-policy.local-datacenter': 'datacenter3'
                ]
        ))
        applicationContext.start()

        expect:
        !applicationContext.getBean(CqlSessionBuilderListener).invoked
        applicationContext.containsBean(CassandraConfiguration)

        when:
        CqlSession defaultSession = applicationContext.getBean(CqlSession, Qualifiers.byName('default'))
        CqlSession secondarySession = applicationContext.getBean(CqlSession, Qualifiers.byName('secondary'))
        CqlSession tertiarySession = applicationContext.getBean(CqlSession, Qualifiers.byName('tertiary'))

        then:
        defaultSession
        secondarySession
        tertiarySession

        when:
        CassandraHealthIndicator healthIndicator = applicationContext.getBean(CassandraHealthIndicator)
        applicationContext.getBean(CqlSessionBuilderListener).invoked
        HealthResult result = Mono.from(healthIndicator.result).block()
        Map<String, Map<String, Object>> detailsMap = (Map<String, Map<String, Object>>) result.details

        then:
        result.status == HealthStatus.UP
        detailsMap.size() == 3
        detailsMap.values().each { {
            it['nodes_count']
            it['nodes_state']
            it['session'] == 'OPEN'
        }}

        when:
        defaultSession.close()
        secondarySession.close()
        tertiarySession.close()
        cassandraContainer.stop()

        result = Mono.from(healthIndicator.result).block()
        detailsMap = (Map<String, Map<String, Object>>) result.details

        then:
        result.status == HealthStatus.DOWN
        detailsMap.values().each { {
            it['session'] == 'CLOSED'
        }}

        cleanup:
        applicationContext.close()
    }

    void "test that CassandraHealthIndicator is not created when the endpoints.health.cassandra.enabled is set to false even if the cqlsession exists"() {
        given:
        CassandraContainer cassandraContainer = new CassandraContainer(DockerImageName.parse("cassandra:latest"))
        cassandraContainer.start()

        // tag::single[]
        ApplicationContext applicationContext = new DefaultApplicationContext("test")
        applicationContext.environment.addPropertySource(MapPropertySource.of(
                'test',
                ['cassandra.default.basic.contact-points'                        : ["localhost:$cassandraContainer.firstMappedPort"],
                 'cassandra.default.advanced.metadata.schema.enabled'            : false,
                 'cassandra.default.basic.load-balancing-policy.local-datacenter': 'datacenter1',
                 'endpoints.health.cassandra.enabled': false]
        ))
        applicationContext.start()
        // end::single[]

        when:
        applicationContext.getBean(CassandraHealthIndicator)


        then:
        thrown(NoSuchBeanException)

        when:
        CqlSession cqlSession = applicationContext.getBean(CqlSession)


        then:
        cqlSession != null

        cleanup:
        applicationContext.close()
        cassandraContainer.stop()
    }

    void "test that CassandraHealthIndicator is not created when no cql session exists"() {
        given:
        // tag::single[]
        ApplicationContext applicationContext = new DefaultApplicationContext("test")
        applicationContext.environment.addPropertySource(MapPropertySource.of(
                'test', Collections.emptyMap()
        ))
        applicationContext.start()
        // end::single[]

        when:
        applicationContext.getBean(CassandraHealthIndicator)


        then:
        thrown(NoSuchBeanException)

        cleanup:
        applicationContext.close()
    }

    @Singleton
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
