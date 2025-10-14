plugins {
    id("io.micronaut.build.internal.cassandra-module")
}

dependencies {
    api(libs.managed.datastax.cassandra.driver.core)
    api(libs.managed.datastax.cassandra.driver.mapper.processor)
    compileOnly(mn.micronaut.management)
    compileOnly(mnMicrometer.micronaut.micrometer.core)

    testImplementation(libs.managed.datastax.cassandra.driver.metrics.micrometer)
    testImplementation(mnTest.micronaut.test.spock)
    testImplementation(mn.reactor)
    testImplementation(platform(mnTestResources.boms.testcontainers))
    testImplementation(mnTestResources.testcontainers.core)
    testImplementation(libs.testcontainers.cassandra)
    testImplementation(mn.micronaut.management)
    testImplementation(mnMicrometer.micronaut.micrometer.core)
    testImplementation(libs.system.stubs)
    testRuntimeOnly(mn.snakeyaml)
}

tasks.withType<Test> {
    // this is needed by libs.system.stubs
    jvmArgs = listOf("--add-opens", "java.base/java.util=ALL-UNNAMED")
}
