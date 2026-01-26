plugins {
    groovy
    id("io.micronaut.build.internal.cassandra-tests")
}

dependencies {
    testImplementation(projects.micronautCassandra)
    testImplementation(mn.micronaut.http.client)
    testImplementation(mn.micronaut.jackson.databind)
    testImplementation(mn.micronaut.management)
    testImplementation(mnReactor.micronaut.reactor)

    testImplementation(libs.bcpkix)
    testImplementation(platform(mnTest.boms.testcontainers))
    testImplementation(libs.testcontainers.cassandra)
    testImplementation(mnTest.micronaut.test.spock)
    testRuntimeOnly(mnLogging.logback.classic)
}

micronaut {
    version.set(libs.versions.micronaut.platform.get())
    runtime("netty")
    testRuntime("spock2")
    processing {
        incremental(true)
        annotations("example.*")
    }
}
