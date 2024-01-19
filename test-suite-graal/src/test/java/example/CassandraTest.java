package example;

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micronaut.context.annotation.Property;
import org.junit.jupiter.api.Test;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;

@MicronautTest
@Property(name = "spec.name", value = "CassandraTest")
class CassandraTest {

    @Test
    void testCassandra(CassandraRepository repository) {
        assertTrue(repository.getInfo().isPresent());
    }

}
