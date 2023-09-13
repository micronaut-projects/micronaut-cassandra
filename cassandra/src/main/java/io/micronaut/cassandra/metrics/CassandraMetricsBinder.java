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
package io.micronaut.cassandra.metrics;

import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import io.micrometer.core.instrument.MeterRegistry;
import io.micronaut.configuration.metrics.annotation.RequiresMetrics;
import io.micronaut.context.BeanProvider;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.event.BeanCreatedEvent;
import io.micronaut.context.event.BeanCreatedEventListener;
import io.micronaut.core.annotation.NonNull;
import jakarta.inject.Singleton;

@RequiresMetrics
@Requires(beans = CqlSessionBuilder.class)
@Singleton
public class CassandraMetricsBinder implements BeanCreatedEventListener<CqlSessionBuilder> {

    private final BeanProvider<MeterRegistry> meterRegistryProvider;

    /**
     * Default constructor.
     *
     * @param meterRegistryProvider The meter registry.
     */
    protected CassandraMetricsBinder(BeanProvider<MeterRegistry> meterRegistryProvider) {
        this.meterRegistryProvider = meterRegistryProvider;
    }

    @Override
    public CqlSessionBuilder onCreated(@NonNull BeanCreatedEvent<CqlSessionBuilder> event) {
        MeterRegistry meterRegistry = meterRegistryProvider.get();
        CqlSessionBuilder cqlSessionBuilder = event.getBean();
        return cqlSessionBuilder.withMetricRegistry(meterRegistry);
    }
}
