package io.micronaut.cassandra.graaltest;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;

import java.util.Optional;

@Singleton
@Requires(property = "spec.name", value = "CassandraTest")
public class CassandraRepository {
    private final CqlSession cqlSession;

    public CassandraRepository(CqlSession cqlSession) {
        this.cqlSession = cqlSession;
    }

    public Optional<CassandraInfo> getInfo() {
        ResultSet resultSet = cqlSession.execute("select cluster_name, release_version from system.local");
        return Optional.ofNullable(resultSet.one())
            .map(r -> new CassandraInfo(r.getString("cluster_name"), r.getString("release_version")));
    }
}
