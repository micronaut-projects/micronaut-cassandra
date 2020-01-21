/*
 * Copyright 2017-2019 original authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.micronaut.configuration.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.EachBean;
import io.micronaut.context.annotation.Factory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Creates cassandra cluster for each configuration bean.
 *
 * @author Nirav Assar
 * @since 1.0
 */
@Factory
public class CassandraSessionFactory implements AutoCloseable{

    private List<CqlSession> sessions = new ArrayList<>(2);
    private static final Logger LOG = LoggerFactory.getLogger(CassandraSessionFactory.class);

    public CassandraSessionFactory(){

    }

    @EachBean(CassandraConfiguration.class)
    public CqlSessionBuilder session(CassandraConfiguration configuration) {

        try {
            CqlSessionBuilder builder = CqlSession.builder().withConfigLoader(new DefaultDriverConfigLoader(() -> {
                Map<String, Object> entires = new HashMap<>(DefaultDriverOption.values().length);
                entires.put(DefaultDriverOption.CONTACT_POINTS.getPath(), configuration.getContactPoints());
                entires.put(DefaultDriverOption.SESSION_NAME.getPath(), configuration.getName());
                entires.put(DefaultDriverOption.SESSION_KEYSPACE.getPath(), configuration.getKeyspace());
                entires.put(DefaultDriverOption.CONFIG_RELOAD_INTERVAL.getPath(), 0);
                // ---------------------------------------------------------------------------------
                CassandraConfiguration.RequestConfigurationProperties requestConfigurationProperties = configuration.getRequest();
                entires.put(DefaultDriverOption.REQUEST_TIMEOUT.getPath(), requestConfigurationProperties.getTimeout());
                entires.put(DefaultDriverOption.REQUEST_CONSISTENCY.getPath(), requestConfigurationProperties.getConsistency());
                entires.put(DefaultDriverOption.REQUEST_PAGE_SIZE.getPath(), requestConfigurationProperties.getPageSize());
                entires.put(DefaultDriverOption.REQUEST_SERIAL_CONSISTENCY.getPath(), requestConfigurationProperties.getConsistencyLevel());
                entires.put(DefaultDriverOption.REQUEST_DEFAULT_IDEMPOTENCE.getPath(), requestConfigurationProperties.isDefaultIdempotency());
                // ---------------------------------------------------------------------------------
//                entires.put(DefaultDriverOption.LOAD_BALANCING_POLICY.getPath(), null);
                CassandraConfiguration.LoadBalancerPolicyProperties loadBalancerPolicyProperties = configuration.getLoadBalancerPolicy();
                entires.put(DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS.getPath(), loadBalancerPolicyProperties.getPolicyClass());
                entires.put(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER.getPath(), loadBalancerPolicyProperties.getLocalDatacenter());
                entires.put(DefaultDriverOption.LOAD_BALANCING_FILTER_CLASS.getPath(), loadBalancerPolicyProperties.getFilterClass());
                // ---------------------------------------------------------------------------------
                entires.put(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT.getPath(), null);
                entires.put(DefaultDriverOption.CONNECTION_SET_KEYSPACE_TIMEOUT.getPath(), null);
                entires.put(DefaultDriverOption.CONNECTION_MAX_REQUESTS.getPath(), null);
                entires.put(DefaultDriverOption.CONNECTION_MAX_ORPHAN_REQUESTS.getPath(), null);
                entires.put(DefaultDriverOption.CONNECTION_WARN_INIT_ERROR.getPath(), null);
                entires.put(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE.getPath(), null);
                entires.put(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE.getPath(), null);
                entires.put(DefaultDriverOption.RECONNECT_ON_INIT.getPath(), null);
                entires.put(DefaultDriverOption.RECONNECTION_POLICY_CLASS.getPath(), null);
                entires.put(DefaultDriverOption.RECONNECTION_BASE_DELAY.getPath(), null);
                entires.put(DefaultDriverOption.RECONNECTION_MAX_DELAY.getPath(), null);
//                entires.put(DefaultDriverOption.RETRY_POLICY.getPath(), null);
                entires.put(DefaultDriverOption.RETRY_POLICY_CLASS.getPath(), null);

//                entires.put(DefaultDriverOption.SPECULATIVE_EXECUTION_POLICY.getPath(), null);
                // -------------- speculative-execution-policy -----------------------------------------
                entires.put(DefaultDriverOption.SPECULATIVE_EXECUTION_POLICY_CLASS.getPath(), null);
                entires.put(DefaultDriverOption.SPECULATIVE_EXECUTION_MAX.getPath(), null);
                entires.put(DefaultDriverOption.SPECULATIVE_EXECUTION_DELAY.getPath(), null);
                // -------------- speculative-execution-policy -----------------------------------------
                entires.put(DefaultDriverOption.AUTH_PROVIDER_CLASS.getPath(), null);
                entires.put(DefaultDriverOption.AUTH_PROVIDER_USER_NAME.getPath(), null);
                entires.put(DefaultDriverOption.AUTH_PROVIDER_PASSWORD.getPath(), null);
                entires.put(DefaultDriverOption.SSL_ENGINE_FACTORY_CLASS.getPath(), null);
                entires.put(DefaultDriverOption.SSL_CIPHER_SUITES.getPath(), null);
                entires.put(DefaultDriverOption.SSL_HOSTNAME_VALIDATION.getPath(), null);
                entires.put(DefaultDriverOption.SSL_KEYSTORE_PATH.getPath(), null);
                entires.put(DefaultDriverOption.SSL_KEYSTORE_PASSWORD.getPath(), null);
                entires.put(DefaultDriverOption.SSL_TRUSTSTORE_PATH.getPath(), null);
                entires.put(DefaultDriverOption.SSL_TRUSTSTORE_PASSWORD.getPath(), null);
                entires.put(DefaultDriverOption.TIMESTAMP_GENERATOR_CLASS.getPath(), null);
                entires.put(DefaultDriverOption.TIMESTAMP_GENERATOR_FORCE_JAVA_CLOCK.getPath(), null);
                entires.put(DefaultDriverOption.TIMESTAMP_GENERATOR_DRIFT_WARNING_INTERVAL.getPath(), null);
                entires.put(DefaultDriverOption.REQUEST_TRACKER_CLASS.getPath(), null);
                entires.put(DefaultDriverOption.REQUEST_LOGGER_SUCCESS_ENABLED.getPath(), null);
                entires.put(DefaultDriverOption.REQUEST_LOGGER_SLOW_THRESHOLD.getPath(), null);
                entires.put(DefaultDriverOption.REQUEST_LOGGER_SLOW_ENABLED.getPath(), null);
                entires.put(DefaultDriverOption.REQUEST_LOGGER_ERROR_ENABLED.getPath(), null);
                entires.put(DefaultDriverOption.REQUEST_LOGGER_MAX_QUERY_LENGTH.getPath(), null);
                entires.put(DefaultDriverOption.REQUEST_LOGGER_VALUES.getPath(), null);
                entires.put(DefaultDriverOption.REQUEST_LOGGER_MAX_VALUE_LENGTH.getPath(), null);
                entires.put(DefaultDriverOption.REQUEST_LOGGER_MAX_VALUES.getPath(), null);
                entires.put(DefaultDriverOption.REQUEST_LOGGER_STACK_TRACES.getPath(), null);
                entires.put(DefaultDriverOption.REQUEST_THROTTLER_CLASS.getPath(), null);
                entires.put(DefaultDriverOption.REQUEST_THROTTLER_MAX_CONCURRENT_REQUESTS.getPath(), null);
                entires.put(DefaultDriverOption.REQUEST_THROTTLER_MAX_REQUESTS_PER_SECOND.getPath(), null);
                entires.put(DefaultDriverOption.REQUEST_THROTTLER_MAX_QUEUE_SIZE.getPath(), null);
                entires.put(DefaultDriverOption.REQUEST_THROTTLER_DRAIN_INTERVAL.getPath(), null);
                entires.put(DefaultDriverOption.METADATA_NODE_STATE_LISTENER_CLASS.getPath(), null);
                entires.put(DefaultDriverOption.METADATA_SCHEMA_CHANGE_LISTENER_CLASS.getPath(), null);
                entires.put(DefaultDriverOption.ADDRESS_TRANSLATOR_CLASS.getPath(), null);
                entires.put(DefaultDriverOption.PROTOCOL_VERSION.getPath(), null);
                entires.put(DefaultDriverOption.PROTOCOL_COMPRESSION.getPath(), null);
                entires.put(DefaultDriverOption.PROTOCOL_MAX_FRAME_LENGTH.getPath(), null);
                entires.put(DefaultDriverOption.REQUEST_WARN_IF_SET_KEYSPACE.getPath(), null);
                entires.put(DefaultDriverOption.REQUEST_TRACE_ATTEMPTS.getPath(), null);
                entires.put(DefaultDriverOption.REQUEST_TRACE_INTERVAL.getPath(), null);
                entires.put(DefaultDriverOption.REQUEST_TRACE_CONSISTENCY.getPath(), null);
                entires.put(DefaultDriverOption.METRICS_SESSION_ENABLED.getPath(), null);
                entires.put(DefaultDriverOption.METRICS_NODE_ENABLED.getPath(), null);
                entires.put(DefaultDriverOption.METRICS_SESSION_CQL_REQUESTS_HIGHEST.getPath(), null);
                entires.put(DefaultDriverOption.METRICS_SESSION_CQL_REQUESTS_DIGITS.getPath(), null);
                entires.put(DefaultDriverOption.METRICS_SESSION_CQL_REQUESTS_INTERVAL.getPath(), null);
                entires.put(DefaultDriverOption.METRICS_SESSION_THROTTLING_HIGHEST.getPath(), null);
                entires.put(DefaultDriverOption.METRICS_SESSION_THROTTLING_DIGITS.getPath(), null);
                entires.put(DefaultDriverOption.METRICS_SESSION_THROTTLING_INTERVAL.getPath(), null);
                entires.put(DefaultDriverOption.METRICS_NODE_CQL_MESSAGES_HIGHEST.getPath(), null);
                entires.put(DefaultDriverOption.METRICS_NODE_CQL_MESSAGES_DIGITS.getPath(), null);
                entires.put(DefaultDriverOption.METRICS_NODE_CQL_MESSAGES_INTERVAL.getPath(), null);
                entires.put(DefaultDriverOption.SOCKET_TCP_NODELAY.getPath(), null);
                entires.put(DefaultDriverOption.SOCKET_KEEP_ALIVE.getPath(), null);
                entires.put(DefaultDriverOption.SOCKET_REUSE_ADDRESS.getPath(), null);
                entires.put(DefaultDriverOption.SOCKET_LINGER_INTERVAL.getPath(), null);
                entires.put(DefaultDriverOption.SOCKET_RECEIVE_BUFFER_SIZE.getPath(), null);
                entires.put(DefaultDriverOption.SOCKET_SEND_BUFFER_SIZE.getPath(), null);
                entires.put(DefaultDriverOption.HEARTBEAT_INTERVAL.getPath(), null);
                entires.put(DefaultDriverOption.HEARTBEAT_TIMEOUT.getPath(), null);
                entires.put(DefaultDriverOption.METADATA_TOPOLOGY_WINDOW.getPath(), null);
                entires.put(DefaultDriverOption.METADATA_TOPOLOGY_MAX_EVENTS.getPath(), null);
                entires.put(DefaultDriverOption.METADATA_SCHEMA_ENABLED.getPath(), null);
                entires.put(DefaultDriverOption.METADATA_SCHEMA_REQUEST_TIMEOUT.getPath(), null);
                entires.put(DefaultDriverOption.METADATA_SCHEMA_REQUEST_PAGE_SIZE.getPath(), null);
                entires.put(DefaultDriverOption.METADATA_SCHEMA_REFRESHED_KEYSPACES.getPath(), null);
                entires.put(DefaultDriverOption.METADATA_SCHEMA_WINDOW.getPath(), null);
                entires.put(DefaultDriverOption.METADATA_SCHEMA_MAX_EVENTS.getPath(), null);
                entires.put(DefaultDriverOption.METADATA_TOKEN_MAP_ENABLED.getPath(), null);
                entires.put(DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT.getPath(), null);
                entires.put(DefaultDriverOption.CONTROL_CONNECTION_AGREEMENT_INTERVAL.getPath(), null);
                entires.put(DefaultDriverOption.CONTROL_CONNECTION_AGREEMENT_TIMEOUT.getPath(), null);
                entires.put(DefaultDriverOption.CONTROL_CONNECTION_AGREEMENT_WARN.getPath(), null);
                entires.put(DefaultDriverOption.PREPARE_ON_ALL_NODES.getPath(), null);
                entires.put(DefaultDriverOption.REPREPARE_ENABLED.getPath(), null);
                entires.put(DefaultDriverOption.REPREPARE_CHECK_SYSTEM_TABLE.getPath(), null);
                entires.put(DefaultDriverOption.REPREPARE_MAX_STATEMENTS.getPath(), null);
                entires.put(DefaultDriverOption.REPREPARE_MAX_PARALLELISM.getPath(), null);
                entires.put(DefaultDriverOption.REPREPARE_TIMEOUT.getPath(), null);
                entires.put(DefaultDriverOption.NETTY_IO_SIZE.getPath(), null);
                entires.put(DefaultDriverOption.NETTY_IO_SHUTDOWN_QUIET_PERIOD.getPath(), null);
                entires.put(DefaultDriverOption.NETTY_IO_SHUTDOWN_TIMEOUT.getPath(), null);
                entires.put(DefaultDriverOption.NETTY_IO_SHUTDOWN_UNIT.getPath(), null);
                entires.put(DefaultDriverOption.NETTY_ADMIN_SIZE.getPath(), null);
                entires.put(DefaultDriverOption.NETTY_ADMIN_SHUTDOWN_QUIET_PERIOD.getPath(), null);
                entires.put(DefaultDriverOption.NETTY_ADMIN_SHUTDOWN_TIMEOUT.getPath(), null);
                entires.put(DefaultDriverOption.NETTY_ADMIN_SHUTDOWN_UNIT.getPath(), null);
                entires.put(DefaultDriverOption.COALESCER_MAX_RUNS.getPath(), null);
                entires.put(DefaultDriverOption.COALESCER_INTERVAL.getPath(), null);
                entires.put(DefaultDriverOption.RESOLVE_CONTACT_POINTS.getPath(), null);
                entires.put(DefaultDriverOption.NETTY_TIMER_TICK_DURATION.getPath(), null);
                entires.put(DefaultDriverOption.NETTY_TIMER_TICKS_PER_WHEEL.getPath(), null);
                entires.put(DefaultDriverOption.REQUEST_LOG_WARNINGS.getPath(), null);
                entires.put(DefaultDriverOption.NETTY_DAEMON.getPath(), null);
                // ---------------------------------------------------------------------------------
                entires.put(DefaultDriverOption.CLOUD_SECURE_CONNECT_BUNDLE.getPath(), null);
                entires.put(DefaultDriverOption.LOAD_BALANCING_POLICY_SLOW_AVOIDANCE.getPath(), null);
                return ConfigFactory.parseMap(entires);
            }));
            return builder;
        } catch (Exception e) {
            LOG.error("Failed to insantiate CQL session: " + e.getMessage(), e);
            throw e;
        }

//        return  builder;
    }

    @EachBean(CqlSessionBuilder.class)
    @Bean(preDestroy = "close")
    public CqlSession cassandraCluster(CqlSessionBuilder builder) {
        return builder.build();
    }

    @Override
    @PreDestroy
    public void close() {
        for(CqlSession sess : sessions){
            try {
                sess.close();
            }
            catch (Exception e){
                if (LOG.isWarnEnabled()) {
                    LOG.warn("Error closing data source [" + sess + "]: " + e.getMessage(), e);
                }
            }
        }
    }
}
