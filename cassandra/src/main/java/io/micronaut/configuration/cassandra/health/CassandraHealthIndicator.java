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

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.api.core.servererrors.QueryExecutionException;
import com.datastax.oss.driver.api.core.servererrors.QueryValidationException;
import io.micronaut.context.annotation.Requires;
import io.micronaut.health.HealthStatus;
import io.micronaut.management.endpoint.health.HealthEndpoint;
import io.micronaut.management.health.indicator.AbstractHealthIndicator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.net.InetSocketAddress;
import java.util.*;

/**
 * A {@link io.micronaut.management.health.indicator.HealthIndicator} for Cassandra.
 *
 * @author Ilkin Ashrafli
 * @since 2.1.0
 */
@Requires(property = HealthEndpoint.PREFIX + ".cassandra.enabled", notEquals = "false")
@Requires(beans = HealthEndpoint.class)
@Singleton
public class CassandraHealthIndicator extends AbstractHealthIndicator<Map<String, Object>> {

    private static final String COL_RELEASE_VERSION = "release_version";
    private static final String COL_CLUSTER_NAME = "cluster_name";
    private static final String COL_CQL_VERSION = "cql_version";
    private static final String COL_DATA_CENTER = "data_center";
    private static final String COL_NATIVE_PROTOCOL_VERSION = "native_protocol_version";
    private static final String COL_PARTITIONER = "partitioner";
    private static final String COL_RACK = "rack";

    private static final SimpleStatement VALIDATION_SELECT = selectFrom("system", "local")
            .column(COL_RELEASE_VERSION)
            .column(COL_CLUSTER_NAME)
            .column(COL_CQL_VERSION)
            .column(COL_DATA_CENTER)
            .column(COL_NATIVE_PROTOCOL_VERSION)
            .column(COL_PARTITIONER)
            .column(COL_RACK)
            .build()
            .setConsistencyLevel(ConsistencyLevel.LOCAL_ONE)
            .setTimeout(Duration.ofSeconds(10));

    private static final Logger LOG = LoggerFactory.getLogger(CassandraHealthIndicator.class);

    private final CqlSession cqlSession;

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
        Map<String, Object> detail = new LinkedHashMap<>();
        Map<UUID, Node> nodes = cqlSession.getMetadata().getNodes();
        detail.put("session", cqlSession.isClosed() ? "CLOSED" : "OPEN");
        Optional<CqlIdentifier> opKeyspace = cqlSession.getKeyspace();
        if (opKeyspace.isPresent()) {
            detail.put("keyspace", opKeyspace.get());
        }
        detail.put("nodes_count", nodes.keySet().size());

        Map<UUID, Map<String, Object>> nodesMap = new HashMap<>();
        Map<NodeState, Integer> nodeStateMap = new EnumMap<>(NodeState.class);
        boolean up = false;
        int i = 0;
        for (Map.Entry<UUID, Node> entry : nodes.entrySet()) {
            UUID uuid = entry.getKey();
            Node node = entry.getValue();
            nodeStateMap.merge(node.getState(), 1, (a, b) -> a + b);
            if (node.getState() == NodeState.UP) {
                up = true;
            }
            if (i++ < 10) {
                Map<String, Object> nodeMap = new HashMap<>();
                Optional<InetSocketAddress> opBroadcastAddress = node.getBroadcastAddress();
                if (opBroadcastAddress.isPresent()) {
                    nodeMap.put("broadcast_address", opBroadcastAddress.get().getAddress());
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
        }
        detail.put("nodes_state", nodeStateMap);
        if (nodesMap.size() > 0) {
            detail.put("nodes", nodesMap);
        }
        healthStatus = up ? HealthStatus.UP : HealthStatus.DOWN;
        try {
            ResultSet resultSet = cqlSession.execute(VALIDATION_SELECT);
            Row row = resultSet.one();
            Map<String, String> clusterMap = new HashMap<>();
            clusterMap.put(COL_CLUSTER_NAME, row.getString(COL_CLUSTER_NAME));
            clusterMap.put(COL_DATA_CENTER, row.getString(COL_DATA_CENTER));
            clusterMap.put(COL_RELEASE_VERSION, row.getString(COL_RELEASE_VERSION));
            clusterMap.put(COL_CQL_VERSION, row.getString(COL_CQL_VERSION));
            clusterMap.put(COL_NATIVE_PROTOCOL_VERSION, row.getString(COL_NATIVE_PROTOCOL_VERSION));
            clusterMap.put(COL_PARTITIONER, row.getString(COL_PARTITIONER));
            clusterMap.put(COL_RACK, row.getString(COL_RACK));
            detail.put("cluster", clusterMap);
        } catch (AllNodesFailedException | QueryExecutionException | QueryValidationException ex) {
            LOG.error(String.format("Failed to execute \"%s\": %s", VALIDATION_SELECT.getQuery(), ex.getMessage()), ex);
        } catch (Exception ex) {
            LOG.error(String.format("Failed to prepare cluster info: %s", ex.getMessage()), ex);
        }
        return detail;
    }

    @Override
    protected String getName() {
        return "cassandra";
    }
}
