package example

import groovy.transform.Canonical
import io.micronaut.core.annotation.Introspected

@Canonical
@Introspected
class CassandraInfo {
    String clusterName
    String releaseVersion
}
