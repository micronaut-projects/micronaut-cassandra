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

    testImplementation(platform(mnTest.boms.testcontainers))
    testImplementation(libs.testcontainers.cassandra)

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

java {
    sourceCompatibility = JavaVersion.toVersion("21")
    targetCompatibility = JavaVersion.toVersion("21")
}
