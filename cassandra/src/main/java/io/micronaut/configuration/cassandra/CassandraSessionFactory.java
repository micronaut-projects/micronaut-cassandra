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
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.typesafe.config.ConfigFactory;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.EachBean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.core.value.PropertyResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Creates cassandra cluster for each configuration bean.
 *
 * @author Nirav Assar
 * @since 1.0
 */
@Factory
public class CassandraSessionFactory implements AutoCloseable {

    private List<CqlSession> sessions = new ArrayList<>(2);
    private static final Logger LOG = LoggerFactory.getLogger(CassandraSessionFactory.class);
    private PropertyResolver resolver;

    public CassandraSessionFactory(PropertyResolver applicationContext) {
        this.resolver = applicationContext;
    }

    private <T> void putConfiguration(Map<String, Object> configurations, String prefix, DefaultDriverOption option, Class<T> requiredType, T defaultValue) {
        configurations.put(option.getPath(), this.resolver.getProperty(CassandraConfiguration.PREFIX + "." + prefix + "." + option.getPath(), requiredType, defaultValue));
    }

    private <T> void putConfiguration(Map<String, Object> configurations, String prefix, DefaultDriverOption option, Class<T> requiredType) {
        Optional<T> value = this.resolver.getProperty(CassandraConfiguration.PREFIX + "." + prefix + "." + option.getPath(), requiredType);
        value.ifPresent(t -> configurations.put(option.getPath(), t));
    }

    private void putDurationMilliseconds(Map<String, Object> configurations, String prefix, DefaultDriverOption option, int defaultValue) {
        Optional<Integer> value = this.resolver.getProperty(CassandraConfiguration.PREFIX + "." + prefix + "." + option.getPath(), int.class);
        configurations.put(option.getPath(), Duration.ofMillis(value.orElse(defaultValue)));
    }

    private void putDurationMilliseconds(Map<String, Object> configurations, String prefix, DefaultDriverOption option) {
        Optional<Integer> value = this.resolver.getProperty(CassandraConfiguration.PREFIX + "." + prefix + "." + option.getPath(), int.class);
        value.ifPresent(t -> configurations.put(option.getPath(), Duration.ofMillis(t)));
    }

    private void putDurationNanoSeconds(Map<String, Object> configurations, String prefix, DefaultDriverOption option, int defaultValue) {

        Optional<Integer> value = this.resolver.getProperty(CassandraConfiguration.PREFIX + "." + prefix + "." + option.getPath(), int.class);
        configurations.put(option.getPath(), Duration.ofNanos(value.orElse(defaultValue)));
    }

    private void putDurationNanoSeconds(Map<String, Object> configurations, String prefix, DefaultDriverOption option) {
        Optional<Integer> value = this.resolver.getProperty(CassandraConfiguration.PREFIX + "." + prefix + "." + option.getPath(), int.class);
        value.ifPresent(t -> configurations.put(option.getPath(), Duration.ofNanos(t)));
    }

    @EachBean(CassandraConfiguration.class)
    public CqlSessionBuilder session(CassandraConfiguration configuration) {

        try {
            CqlSessionBuilder builder = CqlSession.builder().withConfigLoader(new DefaultDriverConfigLoader(() -> {

                Map<String, Object> configurations = new HashMap<>(DefaultDriverOption.values().length);

                String prefix = configuration.getName();
                putConfiguration(configurations, prefix, DefaultDriverOption.CONTACT_POINTS, List.class, Arrays.asList("127.0.0.1:9042", "127.0.0.2:9042"));
                putConfiguration(configurations, prefix, DefaultDriverOption.SESSION_NAME, String.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.SESSION_KEYSPACE, String.class);
                putDurationMilliseconds(configurations, prefix, DefaultDriverOption.CONFIG_RELOAD_INTERVAL, 0);
                putDurationMilliseconds(configurations, prefix, DefaultDriverOption.REQUEST_TIMEOUT, 2000);
                putConfiguration(configurations, prefix, DefaultDriverOption.REQUEST_CONSISTENCY, String.class, "LOCAL_ONE");
                putConfiguration(configurations, prefix, DefaultDriverOption.REQUEST_PAGE_SIZE, int.class, 5000);
                putConfiguration(configurations, prefix, DefaultDriverOption.REQUEST_SERIAL_CONSISTENCY, String.class, "SERIAL");
                putConfiguration(configurations, prefix, DefaultDriverOption.REQUEST_DEFAULT_IDEMPOTENCE, boolean.class, Boolean.FALSE);
                putConfiguration(configurations, prefix, DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS, String.class, "DefaultLoadBalancingPolicy");
                putConfiguration(configurations, prefix, DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, String.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.LOAD_BALANCING_FILTER_CLASS, String.class);
                putDurationMilliseconds(configurations, prefix, DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, 500);
                putDurationMilliseconds(configurations, prefix, DefaultDriverOption.CONNECTION_SET_KEYSPACE_TIMEOUT, 500);
                putConfiguration(configurations, prefix, DefaultDriverOption.CONNECTION_MAX_REQUESTS, int.class, 1024);
                putConfiguration(configurations, prefix, DefaultDriverOption.CONNECTION_MAX_ORPHAN_REQUESTS, int.class, 24576);
                putConfiguration(configurations, prefix, DefaultDriverOption.CONNECTION_WARN_INIT_ERROR, boolean.class, true);
                putConfiguration(configurations, prefix, DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, int.class, 1);
                putConfiguration(configurations, prefix, DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE, int.class, 1);
                putConfiguration(configurations, prefix, DefaultDriverOption.RECONNECT_ON_INIT, Boolean.class, false);
                putConfiguration(configurations, prefix, DefaultDriverOption.RECONNECTION_POLICY_CLASS, String.class, "ExponentialReconnectionPolicy");
                putDurationMilliseconds(configurations, prefix, DefaultDriverOption.RECONNECTION_BASE_DELAY, 1000);
                putDurationMilliseconds(configurations, prefix, DefaultDriverOption.RECONNECTION_MAX_DELAY, 60000);
                putConfiguration(configurations, prefix, DefaultDriverOption.RETRY_POLICY_CLASS, String.class, "DefaultRetryPolicy");
                putConfiguration(configurations, prefix, DefaultDriverOption.SPECULATIVE_EXECUTION_POLICY_CLASS, String.class, "NoSpeculativeExecutionPolicy");
                putConfiguration(configurations, prefix, DefaultDriverOption.SPECULATIVE_EXECUTION_MAX, int.class, 3);
                putDurationMilliseconds(configurations, prefix, DefaultDriverOption.SPECULATIVE_EXECUTION_DELAY, 100);
                putConfiguration(configurations, prefix, DefaultDriverOption.AUTH_PROVIDER_CLASS, String.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.AUTH_PROVIDER_USER_NAME, String.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.AUTH_PROVIDER_PASSWORD, String.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.SSL_ENGINE_FACTORY_CLASS, String.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.SSL_CIPHER_SUITES, List.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.SSL_HOSTNAME_VALIDATION, boolean.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.SSL_KEYSTORE_PATH, String.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.SSL_KEYSTORE_PASSWORD, String.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.SSL_TRUSTSTORE_PATH, String.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.SSL_TRUSTSTORE_PASSWORD, String.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.TIMESTAMP_GENERATOR_CLASS, String.class, "AtomicTimestampGenerator");
                putConfiguration(configurations, prefix, DefaultDriverOption.TIMESTAMP_GENERATOR_FORCE_JAVA_CLOCK, Boolean.class, false);
                putDurationMilliseconds(configurations, prefix, DefaultDriverOption.TIMESTAMP_GENERATOR_DRIFT_WARNING_THRESHOLD, 1000);
                putDurationMilliseconds(configurations, prefix, DefaultDriverOption.TIMESTAMP_GENERATOR_DRIFT_WARNING_INTERVAL, 10000);
                putConfiguration(configurations, prefix, DefaultDriverOption.REQUEST_TRACKER_CLASS, String.class, "NoopRequestTracker");
                putConfiguration(configurations, prefix, DefaultDriverOption.REQUEST_LOGGER_SUCCESS_ENABLED, Boolean.class);
                putDurationMilliseconds(configurations, prefix, DefaultDriverOption.REQUEST_LOGGER_SLOW_THRESHOLD);
                putConfiguration(configurations, prefix, DefaultDriverOption.REQUEST_LOGGER_SLOW_ENABLED, Boolean.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.REQUEST_LOGGER_ERROR_ENABLED, Boolean.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.REQUEST_LOGGER_MAX_QUERY_LENGTH, int.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.REQUEST_LOGGER_VALUES, Boolean.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.REQUEST_LOGGER_MAX_VALUE_LENGTH, int.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.REQUEST_LOGGER_MAX_VALUES, int.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.REQUEST_LOGGER_STACK_TRACES, boolean.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.REQUEST_THROTTLER_CLASS, String.class, "PassThroughRequestThrottler");
                putConfiguration(configurations, prefix, DefaultDriverOption.REQUEST_THROTTLER_MAX_CONCURRENT_REQUESTS, int.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.REQUEST_THROTTLER_MAX_REQUESTS_PER_SECOND, int.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.REQUEST_THROTTLER_MAX_QUEUE_SIZE, int.class);
                putDurationMilliseconds(configurations, prefix, DefaultDriverOption.REQUEST_THROTTLER_DRAIN_INTERVAL);
                putConfiguration(configurations, prefix, DefaultDriverOption.METADATA_NODE_STATE_LISTENER_CLASS, String.class, "NoopNodeStateListener");
                putConfiguration(configurations, prefix, DefaultDriverOption.METADATA_SCHEMA_CHANGE_LISTENER_CLASS, String.class, "NoopSchemaChangeListener");
                putConfiguration(configurations, prefix, DefaultDriverOption.ADDRESS_TRANSLATOR_CLASS, String.class, "PassThroughAddressTranslator");
                putConfiguration(configurations, prefix, DefaultDriverOption.PROTOCOL_VERSION, String.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.PROTOCOL_COMPRESSION, String.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.PROTOCOL_MAX_FRAME_LENGTH, int.class, 256000000);
                putConfiguration(configurations, prefix, DefaultDriverOption.REQUEST_WARN_IF_SET_KEYSPACE, boolean.class, true);
                putConfiguration(configurations, prefix, DefaultDriverOption.REQUEST_TRACE_ATTEMPTS, int.class, 5);
                putDurationMilliseconds(configurations, prefix, DefaultDriverOption.REQUEST_TRACE_INTERVAL, 3);
                putConfiguration(configurations, prefix, DefaultDriverOption.REQUEST_TRACE_CONSISTENCY, String.class, "ONE");
                putConfiguration(configurations, prefix, DefaultDriverOption.METRICS_SESSION_ENABLED, List.class, new ArrayList());
                putConfiguration(configurations, prefix, DefaultDriverOption.METRICS_NODE_ENABLED, List.class, new ArrayList());

                putDurationMilliseconds(configurations, prefix, DefaultDriverOption.METRICS_SESSION_CQL_REQUESTS_HIGHEST, 3000);
                putConfiguration(configurations, prefix, DefaultDriverOption.METRICS_SESSION_CQL_REQUESTS_DIGITS, int.class, 3);
                putDurationMilliseconds(configurations, prefix, DefaultDriverOption.METRICS_SESSION_CQL_REQUESTS_INTERVAL, 300000);

                putDurationMilliseconds(configurations, prefix, DefaultDriverOption.METRICS_SESSION_THROTTLING_HIGHEST, 3000);
                putConfiguration(configurations, prefix, DefaultDriverOption.METRICS_SESSION_THROTTLING_DIGITS, int.class, 3);
                putDurationMilliseconds(configurations, prefix, DefaultDriverOption.METRICS_SESSION_THROTTLING_INTERVAL, 300000);

                putDurationMilliseconds(configurations, prefix, DefaultDriverOption.METRICS_NODE_CQL_MESSAGES_HIGHEST, 3000);
                putConfiguration(configurations, prefix, DefaultDriverOption.METRICS_NODE_CQL_MESSAGES_DIGITS, int.class, 3);
                putDurationMilliseconds(configurations, prefix, DefaultDriverOption.METRICS_NODE_CQL_MESSAGES_INTERVAL, 300000);

                putConfiguration(configurations, prefix, DefaultDriverOption.SOCKET_TCP_NODELAY, Boolean.class, true);
                putConfiguration(configurations, prefix, DefaultDriverOption.SOCKET_KEEP_ALIVE, Boolean.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.SOCKET_REUSE_ADDRESS, Boolean.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.SOCKET_LINGER_INTERVAL, int.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.SOCKET_RECEIVE_BUFFER_SIZE, int.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.SOCKET_SEND_BUFFER_SIZE, int.class);
                putDurationMilliseconds(configurations, prefix, DefaultDriverOption.HEARTBEAT_INTERVAL, 30000);
                putDurationMilliseconds(configurations, prefix, DefaultDriverOption.HEARTBEAT_TIMEOUT, 500);
                putDurationMilliseconds(configurations, prefix, DefaultDriverOption.METADATA_TOPOLOGY_WINDOW, 1000);
                putConfiguration(configurations, prefix, DefaultDriverOption.METADATA_TOPOLOGY_MAX_EVENTS, int.class, 20);
                putConfiguration(configurations, prefix, DefaultDriverOption.METADATA_SCHEMA_ENABLED, boolean.class, true);
                putDurationMilliseconds(configurations, prefix, DefaultDriverOption.METADATA_SCHEMA_REQUEST_TIMEOUT, 2000);

                putConfiguration(configurations, prefix, DefaultDriverOption.METADATA_SCHEMA_REQUEST_PAGE_SIZE, List.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.METADATA_SCHEMA_REFRESHED_KEYSPACES, List.class, new ArrayList());
                putDurationMilliseconds(configurations, prefix, DefaultDriverOption.METADATA_SCHEMA_WINDOW, 1000);
                putConfiguration(configurations, prefix, DefaultDriverOption.METADATA_SCHEMA_MAX_EVENTS, int.class, 20);
                putConfiguration(configurations, prefix, DefaultDriverOption.METADATA_TOKEN_MAP_ENABLED, Boolean.class, true);
                putDurationMilliseconds(configurations, prefix, DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT, 500);
                putDurationMilliseconds(configurations, prefix, DefaultDriverOption.CONTROL_CONNECTION_AGREEMENT_INTERVAL, 200);
                putDurationMilliseconds(configurations, prefix, DefaultDriverOption.CONTROL_CONNECTION_AGREEMENT_TIMEOUT, 10000);
                putConfiguration(configurations, prefix, DefaultDriverOption.CONTROL_CONNECTION_AGREEMENT_WARN, boolean.class, true);
                putConfiguration(configurations, prefix, DefaultDriverOption.PREPARE_ON_ALL_NODES, boolean.class, true);
                putConfiguration(configurations, prefix, DefaultDriverOption.REPREPARE_ENABLED, boolean.class, true);
                putConfiguration(configurations, prefix, DefaultDriverOption.REPREPARE_CHECK_SYSTEM_TABLE, boolean.class, false);
                putConfiguration(configurations, prefix, DefaultDriverOption.REPREPARE_MAX_STATEMENTS, int.class, 0);
                putConfiguration(configurations, prefix, DefaultDriverOption.REPREPARE_MAX_PARALLELISM, int.class, 100);
                putDurationMilliseconds(configurations, prefix, DefaultDriverOption.REPREPARE_TIMEOUT, 500);
                putConfiguration(configurations, prefix, DefaultDriverOption.NETTY_IO_SIZE, int.class, 0);
                putConfiguration(configurations, prefix, DefaultDriverOption.NETTY_IO_SHUTDOWN_QUIET_PERIOD, int.class, 2);
                putConfiguration(configurations, prefix, DefaultDriverOption.NETTY_IO_SHUTDOWN_TIMEOUT, int.class, 15);
                putConfiguration(configurations, prefix, DefaultDriverOption.NETTY_IO_SHUTDOWN_UNIT, String.class, "SECONDS");
                putConfiguration(configurations, prefix, DefaultDriverOption.NETTY_ADMIN_SIZE, int.class, 2);
                putConfiguration(configurations, prefix, DefaultDriverOption.NETTY_ADMIN_SHUTDOWN_QUIET_PERIOD, int.class, 2);
                putConfiguration(configurations, prefix, DefaultDriverOption.NETTY_ADMIN_SHUTDOWN_TIMEOUT, int.class, 15);
                putConfiguration(configurations, prefix, DefaultDriverOption.NETTY_ADMIN_SHUTDOWN_UNIT, String.class, "SECONDS");
                putConfiguration(configurations, prefix, DefaultDriverOption.COALESCER_MAX_RUNS, int.class, 5);
                putDurationNanoSeconds(configurations, prefix, DefaultDriverOption.COALESCER_INTERVAL, 10000);
                putConfiguration(configurations, prefix, DefaultDriverOption.RESOLVE_CONTACT_POINTS, boolean.class, true);
                putDurationMilliseconds(configurations, prefix, DefaultDriverOption.NETTY_TIMER_TICK_DURATION, 1);
                putConfiguration(configurations, prefix, DefaultDriverOption.NETTY_TIMER_TICKS_PER_WHEEL, int.class, 2048);
                putConfiguration(configurations, prefix, DefaultDriverOption.REQUEST_LOG_WARNINGS, boolean.class, true);
                putConfiguration(configurations, prefix, DefaultDriverOption.NETTY_DAEMON, boolean.class, false);
                putConfiguration(configurations, prefix, DefaultDriverOption.CLOUD_SECURE_CONNECT_BUNDLE, String.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.LOAD_BALANCING_POLICY_SLOW_AVOIDANCE, boolean.class, false);

                return ConfigFactory.parseMap(configurations);
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
        CqlSession session = builder.build();
        this.sessions.add(session);
        return session;
    }

    @Override
    @PreDestroy
    public void close() {
        for (CqlSession sess : sessions) {
            try {
                sess.close();
            } catch (Exception e) {
                if (LOG.isWarnEnabled()) {
                    LOG.warn("Error closing data source [" + sess + "]: " + e.getMessage(), e);
                }
            }
        }
    }
}
