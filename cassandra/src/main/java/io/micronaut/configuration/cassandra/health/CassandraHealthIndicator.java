/*
 * Copyright 2017-2020 original authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.micronaut.configuration.cassandra.health;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;

import com.datastax.oss.driver.api.core.metadata.Node;
import io.micronaut.context.annotation.Requires;
import io.micronaut.health.HealthStatus;
import io.micronaut.management.endpoint.health.HealthEndpoint;
import io.micronaut.management.health.indicator.AbstractHealthIndicator;

import javax.inject.Singleton;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * A {@link io.micronaut.management.health.indicator.HealthIndicator} for Cassandra.
 *
 * @author Ilkin Ashrafli
 * @since 2.0.1
 */
@Requires(property = HealthEndpoint.PREFIX + ".cassandra.enabled", notEquals = "false")
@Requires(beans = HealthEndpoint.class)
@Singleton
public class CassandraHealthIndicator extends AbstractHealthIndicator<Map<String, Object>> {

    private final CqlSession cqlSession;
    private static final SimpleStatement VALIDATION_SELECT = selectFrom("system", "local")
            .column("key")
            .column("release_version")
            .column("cluster_name")
            .column("cql_version")
            .column("data_center")
            .column("native_protocol_version")
            .column("partitioner")
            .column("rack")
            .build()
            .setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);

    /**
     * Default constructor.
     *
     * @param cqlSession The The cassandra {@link CqlSession} to query for details
     */
    public CassandraHealthIndicator(final CqlSession cqlSession) {
        this.cqlSession = cqlSession;
    }


    @Override
    protected Map<String, Object> getHealthInformation() {
        ResultSet resultSet = cqlSession.execute(VALIDATION_SELECT);
        Row row = resultSet.one();
        Map<String, Object> detail = new LinkedHashMap<>(3);
        detail.put("session", getStatus(cqlSession.isClosed()));
        if (cqlSession.getKeyspace().isPresent()) {
            detail.put("keyspace", cqlSession.getKeyspace().get());
        }
        detail.put("cluster_name", row.getString("cluster_name"));
        detail.put("key", row.getString("key"));
        detail.put("data_center", row.getString("data_center"));
        detail.put("release_version", row.getString("release_version"));
        detail.put("cql_version", row.getString("cql_version"));
        detail.put("native_protocol_version", row.getString("native_protocol_version"));
        detail.put("partitioner", row.getString("partitioner"));
        detail.put("rack", row.getString("rack"));

        Map<UUID, Node> nodes = cqlSession.getMetadata().getNodes();
        Map<UUID, Map<String, Object>> nodesMap = new HashMap<>();
        for (UUID uuid : nodes.keySet()) {
            Node node = nodes.get(uuid);
            Map<String, Object> nodeMap = new HashMap<>();
            if (node.getBroadcastAddress().isPresent()) {
                nodeMap.put("broadcast_address", node.getBroadcastAddress().get().getAddress());
            }
            nodeMap.put("state", node.getState());
            nodeMap.put("distance", node.getDistance());
            nodeMap.put("open_connections", node.getOpenConnections());
            nodeMap.put("cassandra_version", node.getCassandraVersion());
            nodeMap.put("datacenter", node.getDatacenter());
            nodeMap.put("rack", node.getRack());
            nodeMap.put("uptime_ms", node.getUpSinceMillis());
            nodeMap.put("is_reconnecting", node.isReconnecting());
            nodesMap.put(uuid, nodeMap);
        }
        detail.put("nodes", nodesMap);

        healthStatus = HealthStatus.UP;
        return detail;
    }

    private static String getStatus(boolean closed) {
        return closed ? "CLOSED" : "OPEN";
    }

    @Override
    protected String getName() {
        return "cassandra";
    }
}
