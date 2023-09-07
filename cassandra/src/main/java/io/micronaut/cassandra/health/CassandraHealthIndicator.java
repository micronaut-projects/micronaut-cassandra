/*
 * Copyright 2017-2023 original authors
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
package io.micronaut.cassandra.health;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import io.micronaut.context.annotation.Requires;
import io.micronaut.core.annotation.Introspected;
import io.micronaut.health.HealthStatus;
import io.micronaut.management.endpoint.health.HealthEndpoint;
import io.micronaut.management.health.indicator.AbstractHealthIndicator;
import jakarta.inject.Singleton;

import java.net.InetSocketAddress;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * A {@link io.micronaut.management.health.indicator.HealthIndicator} for Cassandra, handling multiple Configurations.
 *
 * @author Ilkin Ashrafli
 * @since 2.2.0
 */
@Requires(property = HealthEndpoint.PREFIX + ".cassandra.enabled", notEquals = "false")
@Requires(beans = {HealthEndpoint.class, CqlSession.class})
@Introspected
@Singleton
public class CassandraHealthIndicator extends AbstractHealthIndicator<Map<String, Object>> {

    private final List<CqlSession> cqlSessions;

    /**
     * Default constructor.
     *
     * @param cqlSessions The list of cassandra {@link CqlSession} to query for details
     */
    public CassandraHealthIndicator(final List<CqlSession> cqlSessions) {
        this.cqlSessions = cqlSessions;
    }

    @Override
    protected Map<String, Object> getHealthInformation() {
        healthStatus = null;
        return cqlSessions.stream().collect(Collectors.toMap(
            CqlSession::getName, this::getHealthInformation, (a, b) -> b, LinkedHashMap::new));
    }

    private Map<String, Object> getHealthInformation(CqlSession cqlSession) {
        Map<String, Object> detail = new LinkedHashMap<>();
        Map<UUID, Node> nodes = cqlSession.getMetadata().getNodes();
        detail.put("session", cqlSession.isClosed() ? "CLOSED" : "OPEN");
        Optional<String> opClusterName = cqlSession.getMetadata().getClusterName();
        opClusterName.ifPresent(s -> detail.put("cluster_name", s));
        Optional<CqlIdentifier> opKeyspace = cqlSession.getKeyspace();
        opKeyspace.ifPresent(cqlIdentifier -> detail.put("keyspace", cqlIdentifier.asInternal()));
        detail.put("nodes_count", nodes.keySet().size());

        Map<UUID, Map<String, Object>> nodesMap = new HashMap<>();
        Map<NodeState, Integer> nodeStateMap = new EnumMap<>(NodeState.class);
        boolean up = false;
        int i = 0;
        for (Map.Entry<UUID, Node> entry : nodes.entrySet()) {
            UUID uuid = entry.getKey();
            Node node = entry.getValue();
            nodeStateMap.merge(node.getState(), 1, Integer::sum);
            if (node.getState() == NodeState.UP) {
                up = true;
            }
            if (i++ < 10) {
                Map<String, Object> nodeMap = new HashMap<>();
                Optional<InetSocketAddress> opBroadcastAddress = node.getBroadcastAddress();
                opBroadcastAddress.ifPresent(inetSocketAddress -> nodeMap.put("broadcast_address", inetSocketAddress.getAddress()));
                nodeMap.put("endpoint", node.getEndPoint());
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
        if (!nodesMap.isEmpty()) {
            detail.put("nodes (10 max.)", nodesMap);
        }

        // this needs work, it says status is whatever the status of the last node tested
        // which seems clearly wrong
        healthStatus = up ? HealthStatus.UP : HealthStatus.DOWN;
        return detail;
    }

    @Override
    protected String getName() {
        return "cassandra";
    }
}
