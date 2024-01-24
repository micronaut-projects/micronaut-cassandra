package io.micronaut.cassandra.ssltest

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.ResultSet
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton

@Requires(property = 'spec.name', value = 'CassandraSSLConfigSpec')
@Singleton
class CassandraRepository {

    CqlSession cqlSession

    CassandraRepository(CqlSession cqlSession) {
        this.cqlSession = cqlSession
    }

    Optional<CassandraInfo> getInfo() {
        ResultSet resultSet = cqlSession.execute('select cluster_name, release_version from system.local')
        Optional.ofNullable(resultSet.one()).map(r -> new CassandraInfo(r.getString('cluster_name'), r.getString('release_version')))
    }
}
