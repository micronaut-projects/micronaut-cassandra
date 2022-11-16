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
import io.micronaut.management.health.indicator.HealthResult
import org.testcontainers.containers.CassandraContainer
import reactor.core.publisher.Mono
import spock.lang.Specification

import jakarta.inject.Singleton

/**
 * @author Ilkin Ashrafli
 * @since 2.2.0
 */
class CassandraHealthIndicatorSpec extends Specification {

    void "test cassandra health indicator"() {
        given:
        CassandraContainer cassandraContainer = new CassandraContainer()
        cassandraContainer.start()

        // tag::single[]
        ApplicationContext applicationContext = new DefaultApplicationContext("test")
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

        then:
        result.status == HealthStatus.UP
        Map<String, Object> detailsMap = (Map<String, Object>) result.details
        detailsMap.containsKey("nodes_count")
        detailsMap.containsKey("nodes_state")
        detailsMap.get("session").toString().startsWith("OPEN")

        when:
        cassandraContainer.stop()
        result = Mono.from(healthIndicator.result).block()

        then:
        result.status == HealthStatus.DOWN

        cleanup:
        applicationContext.close()
    }

    void "test that CassandraHealthIndicator is not created when the endpoints.health.cassandra.enabled is set to false even if the cqlsession exists"() {
        given:
        CassandraContainer cassandraContainer = new CassandraContainer()
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
