plugins {
    id("io.micronaut.test-resources")
    id("io.micronaut.build.internal.cassandra-native-tests")
}

micronaut {
    importMicronautPlatform = false
    testRuntime("junit5")
    enableNativeImage(false)
    processing {
        incremental(true)
        annotations("io.micronaut.cassandra.graaltest")
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

    testRuntimeOnly(mn.snakeyaml)
    testRuntimeOnly(mnLogging.logback.classic)
    testImplementation(platform(mnTestResources.boms.testcontainers))
    testImplementation(libs.testcontainers.junit.jupiter)
}
