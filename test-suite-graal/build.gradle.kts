plugins {
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
}

dependencies {
    testImplementation(projects.micronautCassandra)

    testImplementation(mn.micronaut.context)
    testImplementation(mn.micronaut.jackson.databind)
    testImplementation(mnTest.micronaut.test.junit5)

    testRuntimeOnly(mn.snakeyaml)
    testRuntimeOnly(mnLogging.logback.classic)
    testImplementation(platform(mnTest.boms.testcontainers))
    testImplementation(libs.testcontainers)
    testImplementation(libs.testcontainers.cassandra)
    testImplementation(libs.testcontainers.junit.jupiter)
}

graalvmNative {
    binaries.all {
        buildArgs.add("--initialize-at-build-time=com.datastax.oss.driver.shaded.guava.common.primitives.UnsignedBytes\$LexicographicalComparatorHolder\$PureJavaComparator")
    }
}

