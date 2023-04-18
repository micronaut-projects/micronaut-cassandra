package example;

import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;

@MicronautTest
class CassandraTest {

    @Test
    void testCassandra(CassandraRepository repository) {
        assertTrue(repository.getInfo().isPresent());
    }

}