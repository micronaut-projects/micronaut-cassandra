package io.micronaut.cassandra.micrometertest

import groovy.transform.Canonical
import io.micronaut.core.annotation.Introspected

@Canonical
@Introspected
class CassandraInfo {

    String clusterName
    String releaseVersion
}
