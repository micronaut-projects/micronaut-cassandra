package io.micronaut.cassandra.graaltest;

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micronaut.context.annotation.Property;
import io.micronaut.test.support.TestPropertyProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.DisabledInNativeImage;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Map;

@MicronautTest
@Property(name = "spec.name", value = "CassandraTest")
@Testcontainers(disabledWithoutDocker = true)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CassandraTest implements TestPropertyProvider {

    @Override
    public Map<String, String> getProperties() {
        return Cassandra.getProperties();
    }

    @DisabledInNativeImage
    @Test
    void testCassandra(CassandraRepository repository) {
        assertTrue(repository.getInfo().isPresent());
    }

}
