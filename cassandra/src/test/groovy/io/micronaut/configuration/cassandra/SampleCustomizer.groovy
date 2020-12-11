package io.micronaut.configuration.cassandra


import com.datastax.oss.driver.api.core.CqlSessionBuilder
import io.micronaut.context.annotation.Requires

import javax.inject.Singleton

@Singleton
@Requires(beans = CqlSessionBuilder.class, property = "cassandra.customizer", value = "true")
class SampleCustomizer implements CqlSessionCustomizer {
    public static final String LOCAL_DC = "my-different-dc"

    @Override
    CqlSessionBuilder customize(CqlSessionBuilder cqlSessionBuilder) {
        return cqlSessionBuilder.withLocalDatacenter(LOCAL_DC)
    }
}
