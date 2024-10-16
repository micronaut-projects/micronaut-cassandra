plugins {
    groovy
    id("io.micronaut.test-resources")
    id("io.micronaut.build.internal.cassandra-tests")
}

micronaut {
    version.set(libs.versions.micronaut.platform.get())
    runtime("netty")
    testRuntime("spock")
    processing {
        incremental(true)
        annotations("example.*")
    }
    testResources {
        clientTimeout = 600
    }
}

dependencies {
    testImplementation(projects.micronautCassandra)
    testImplementation(mn.micronaut.context)
    testImplementation(mn.micronaut.jackson.databind)
    testImplementation(mnTest.micronaut.test.junit5)
    testImplementation(mnMicrometer.micronaut.micrometer.core)
    testImplementation(libs.managed.datastax.cassandra.driver.metrics.micrometer)
    testRuntimeOnly(mnLogging.logback.classic)
    testImplementation(mnTestResources.testcontainers.core)
}

