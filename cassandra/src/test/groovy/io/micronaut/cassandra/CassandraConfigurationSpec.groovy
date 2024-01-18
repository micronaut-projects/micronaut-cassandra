/*
 * Copyright 2017-2019 original authors
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
package io.micronaut.cassandra

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.CqlSessionBuilder
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy
import com.datastax.oss.driver.internal.core.loadbalancing.DefaultLoadBalancingPolicy
import io.micronaut.context.ApplicationContext
import io.micronaut.context.DefaultApplicationContext
import io.micronaut.context.env.MapPropertySource
import io.micronaut.context.event.BeanCreatedEvent
import io.micronaut.context.event.BeanCreatedEventListener
import io.micronaut.inject.qualifiers.Qualifiers
import org.testcontainers.containers.CassandraContainer
import org.testcontainers.utility.DockerImageName
import spock.lang.Specification

import jakarta.inject.Singleton

class CassandraConfigurationSpec extends Specification {

    void "test no configuration"() {
        given:
        ApplicationContext applicationContext = ApplicationContext.run()

        expect: "No beans are created"
        !applicationContext.containsBean(CassandraConfiguration)
        !applicationContext.containsBean(CqlSession)

        cleanup:
        applicationContext.close()
    }

    void "test single cluster connection"() {
        given:
        CassandraContainer cassandra = new CassandraContainer(DockerImageName.parse('cassandra:latest'))
        cassandra.start()
        // tag::single[]
        ApplicationContext applicationContext = new DefaultApplicationContext("test")
        applicationContext.environment.addPropertySource(MapPropertySource.of(
                'test',
                ['cassandra.default.basic.contact-points'                        : ["localhost:$cassandra.firstMappedPort"],
                 'cassandra.default.advanced.metadata.schema.enabled'            : false,
                 'cassandra.default.basic.load-balancing-policy.local-datacenter': 'ociCluster']
        ))
        applicationContext.start()
        // end::single[]

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

        then:
        cassandra.stop()
        applicationContext.close()
        session.isClosed()
    }

    void "test multiple cluster connections"() {
        given:
        CassandraContainer cassandra = new CassandraContainer(DockerImageName.parse('cassandra:latest'))
        cassandra.start()

        // tag::multiple[]
        ApplicationContext applicationContext = new DefaultApplicationContext("test")
        applicationContext.environment.addPropertySource(MapPropertySource.of(
                'test',
                ['cassandra.default.basic.contact-points'                          : ["localhost:$cassandra.firstMappedPort"],
                 'cassandra.default.advanced.metadata.schema.enabled'              : true,
                 'cassandra.default.basic.load-balancing-policy.local-datacenter'  : 'ociCluster',
                 'cassandra.secondary.basic.contact-points'                        : ["localhost:$cassandra.firstMappedPort"],
                 'cassandra.secondary.advanced.metadata.schema.enabled'            : false,
                 'cassandra.secondary.basic.load-balancing-policy.local-datacenter': 'ociCluster2']
        ))
        applicationContext.start()
        // end::multiple[]

        when:
        CqlSession defaultCluster = applicationContext.getBean(CqlSession)
        CqlSession secondaryCluster = applicationContext.getBean(CqlSession, Qualifiers.byName("secondary"))
        Collection<LoadBalancingPolicy> defaultPolicies = defaultCluster.getContext().loadBalancingPolicies.values()
        Collection<LoadBalancingPolicy> secondaryPolicies = secondaryCluster.getContext().loadBalancingPolicies.values()

        then:
        ((DefaultLoadBalancingPolicy) defaultPolicies[0]).getLocalDatacenter() == "ociCluster"
        ((DefaultLoadBalancingPolicy) secondaryPolicies[0]).getLocalDatacenter() == "ociCluster2"
        defaultCluster.schemaMetadataEnabled
        !secondaryCluster.schemaMetadataEnabled

        cleanup:
        cassandra.stop()
        applicationContext.close()
        defaultCluster.isClosed()
        secondaryCluster.isClosed()
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
