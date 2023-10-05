package example

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.ResultSet
import com.datastax.oss.driver.api.core.cql.Row
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton

@Requires(property = 'spec.name', value = 'CassandraMetricsSpec')
@Singleton
class CassandraRepository {
    CqlSession cqlSession

    CassandraRepository(CqlSession cqlSession) {
        this.cqlSession = cqlSession
    }

    Optional<CassandraInfo> getInfo() {
        ResultSet resultSet = cqlSession.execute('select cluster_name, release_version from system.local')
        Row row = resultSet.one()

        if (row != null) {
            Optional.of(
                    new CassandraInfo(row.getString('cluster_name'), row.getString('release_version'))
            )
        }
        Optional.empty()
    }
}
