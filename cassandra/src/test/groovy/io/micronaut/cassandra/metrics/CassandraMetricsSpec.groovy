/*
 * Copyright 2017-2023 original authors
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
package io.micronaut.cassandra.metrics

import com.datastax.oss.driver.api.core.CqlSession
import io.micrometer.core.instrument.MeterRegistry
import io.micronaut.context.ApplicationContext
import io.micronaut.context.DefaultApplicationContext
import io.micronaut.context.env.MapPropertySource
import org.testcontainers.containers.CassandraContainer
import org.testcontainers.utility.DockerImageName
import spock.lang.Specification

class CassandraMetricsSpec extends Specification {

    void "test metrics"() {
        given:
        CassandraContainer cassandra = new CassandraContainer(DockerImageName.parse('cassandra:latest'))
        cassandra.start()

        ApplicationContext applicationContext = new DefaultApplicationContext("test")
        applicationContext.environment.addPropertySource(MapPropertySource.of(
                'test',
                ['cassandra.default.basic.contact-points'                           : ["localhost:$cassandra.firstMappedPort"],
                 'cassandra.default.advanced.metadata.schema.enabled'               : true,
                 'micronaut.metrics.enabled'                                        : true,
                 'cassandra.default.advanced.metrics.factory.class'                 : 'MicrometerMetricsFactory',
                 'cassandra.default.basic.load-balancing-policy.local-datacenter'   : 'ociCluster'
                ]
        ))
        applicationContext.start()

        when:
        CqlSession defaultCluster = applicationContext.getBean(CqlSession)
        MeterRegistry meterRegistry = applicationContext.getBean(MeterRegistry)


        then:
        defaultCluster
        meterRegistry

        cleanup:
        cassandra.stop()
        applicationContext.close()
    }
}
