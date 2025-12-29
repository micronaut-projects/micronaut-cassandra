package io.micronaut.cassandra.graaltest;

import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;

public class Cassandra {

    private static final String IMAGE_NAME = "cassandra:3.11.2";
    private static CassandraContainer container;

    public static Map<String, String> getProperties() {
        if (container == null) {
            container = new CassandraContainer(DockerImageName.parse(IMAGE_NAME));
            container.start();
            do {
                try {
                    Thread.sleep(600);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            } while(!container.isRunning());
            return getProperties(container);
        } else {
            return getProperties(container);
        }
    }

    private static Map<String, String> getProperties(CassandraContainer container) {
        return Map.of(
            "cassandra.port", String.valueOf(container.getMappedPort(9042))
        );
    }
}
