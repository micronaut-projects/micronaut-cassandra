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
import com.typesafe.config.ConfigFactory;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.EachBean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.core.value.PropertyResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Creates cassandra cluster for each configuration bean.
 *
 * @author Nirav Assar
 * @author Michael Pollind
 * @since 1.0
 */
@Factory
public class CassandraSessionFactory implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraSessionFactory.class);
    private List<CqlSession> sessions = new ArrayList<>(2);
    private PropertyResolver resolver;

    /**
     * Default constructor.
     *
     * @param propertyResolver Property resolve for application configurations
     */
    public CassandraSessionFactory(PropertyResolver propertyResolver) {
        this.resolver = propertyResolver;
    }


    /**
     * Inserts configuration from application context into {@link com.typesafe.config.Config} mapping path from cassandra.datasource.* to {@link DefaultDriverOption} path by type.
     * Uses default value if default value if not present in {@link PropertyResolver}.
     *
     * @param configurations map of paths from {@link com.typesafe.config.Config} to values to use in cassandra driver
     * @param option         driver option from cassandra driver
     */
    private <T> void putConfiguration(Map<String, Object> configurations, String prefix, DefaultDriverOption option, Class<T> requiredType, T defaultValue) {
        configurations.put(option.getPath(), this.resolver.getProperty(CassandraConfiguration.PREFIX + "." + prefix + "." + option.getPath(), requiredType, defaultValue));
    }

    /**
     * Inserts configuration from application context into {@link com.typesafe.config.Config} mapping path from cassandra.datasource.* to {@link DefaultDriverOption} path by type.
     * Ignore property if not present in {@link PropertyResolver}.
     *
     * @param configurations map of paths from {@link com.typesafe.config.Config} to values to use in cassandra driver
     * @param option         driver option from cassandra driver
     */
    private <T> void putConfiguration(Map<String, Object> configurations, String prefix, DefaultDriverOption option, Class<T> requiredType) {
        Optional<T> value = this.resolver.getProperty(CassandraConfiguration.PREFIX + "." + prefix + "." + option.getPath(), requiredType);
        value.ifPresent(t -> configurations.put(option.getPath(), t));
    }

    /**
     * Inserts configuration from application context into {@link com.typesafe.config.Config} mapping path from cassandra.datasource.* to {@link DefaultDriverOption} path by type.
     * Maps int to duration by milliseconds with default value if not present in {@link PropertyResolver}.
     *
     * @param configurations map of paths from {@link com.typesafe.config.Config} to values to use in cassandra driver
     * @param option         driver option from cassandra driver
     */
    private void putDurationMilliseconds(Map<String, Object> configurations, String prefix, DefaultDriverOption option, int defaultValue) {
        Optional<Integer> value = this.resolver.getProperty(CassandraConfiguration.PREFIX + "." + prefix + "." + option.getPath(), int.class);
        configurations.put(option.getPath(), Duration.ofMillis(value.orElse(defaultValue)));
    }

    /**
     * Inserts configuration from application context into {@link com.typesafe.config.Config} mapping path from cassandra.datasource.* to {@link DefaultDriverOption} path by type.
     * Maps int to duration by milliseconds.
     *
     * @param configurations map of paths from {@link com.typesafe.config.Config} to values to use in cassandra driver
     * @param option         driver option from cassandra driver
     */
    private void putDurationMilliseconds(Map<String, Object> configurations, String prefix, DefaultDriverOption option) {
        Optional<Integer> value = this.resolver.getProperty(CassandraConfiguration.PREFIX + "." + prefix + "." + option.getPath(), int.class);
        value.ifPresent(t -> configurations.put(option.getPath(), Duration.ofMillis(t)));
    }

    /**
     * Inserts configuration from application context into {@link com.typesafe.config.Config} mapping path from cassandra.datasource.* to {@link DefaultDriverOption} path by type.
     * Maps int to duration by nanoseconds with default value if not present in {@link PropertyResolver}.
     *
     * @param configurations map of paths from {@link com.typesafe.config.Config} to values to use in cassandra driver
     * @param option         driver option from cassandra driver
     */
    private void putDurationNanoSeconds(Map<String, Object> configurations, String prefix, DefaultDriverOption option, int defaultValue) {

        Optional<Integer> value = this.resolver.getProperty(CassandraConfiguration.PREFIX + "." + prefix + "." + option.getPath(), int.class);
        configurations.put(option.getPath(), Duration.ofNanos(value.orElse(defaultValue)));
    }

    /**
     * Inserts configuration from application context into {@link com.typesafe.config.Config} mapping path from cassandra.datasource.* to {@link DefaultDriverOption} path by type.
     * Maps int to duration by nanoseconds.
     */
    private void putDurationNanoSeconds(Map<String, Object> configurations, String prefix, DefaultDriverOption option) {
        Optional<Integer> value = this.resolver.getProperty(CassandraConfiguration.PREFIX + "." + prefix + "." + option.getPath(), int.class);
        value.ifPresent(t -> configurations.put(option.getPath(), Duration.ofNanos(t)));
    }

    /**
     * Creates the {@link CqlSessionBuilder} bean for the given configuration.
     *
     * @param configuration The cassandra configuration bean
     * @return A {@link CqlSession} bean
     */
    @EachBean(CassandraConfiguration.class)
    public CqlSessionBuilder session(CassandraConfiguration configuration) {

        try {
            CqlSessionBuilder builder = CqlSession.builder().withConfigLoader(new DefaultDriverConfigLoader(() -> {
                Map<String, Object> configurations = new HashMap<>(DefaultDriverOption.values().length);
                String prefix = configuration.getName();
                putConfiguration(configurations, prefix, DefaultDriverOption.CONTACT_POINTS, List.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.SESSION_NAME, String.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.SESSION_KEYSPACE, String.class);
                putDurationMilliseconds(configurations, prefix, DefaultDriverOption.CONFIG_RELOAD_INTERVAL, 0); // disable configuration reloading
                putDurationMilliseconds(configurations, prefix, DefaultDriverOption.REQUEST_TIMEOUT);
                putConfiguration(configurations, prefix, DefaultDriverOption.REQUEST_CONSISTENCY, String.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.REQUEST_PAGE_SIZE, int.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.REQUEST_SERIAL_CONSISTENCY, String.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.REQUEST_DEFAULT_IDEMPOTENCE, boolean.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS, String.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, String.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.LOAD_BALANCING_FILTER_CLASS, String.class);
                putDurationMilliseconds(configurations, prefix, DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT);
                putDurationMilliseconds(configurations, prefix, DefaultDriverOption.CONNECTION_SET_KEYSPACE_TIMEOUT);
                putConfiguration(configurations, prefix, DefaultDriverOption.CONNECTION_MAX_REQUESTS, int.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.CONNECTION_MAX_ORPHAN_REQUESTS, int.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.CONNECTION_WARN_INIT_ERROR, boolean.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, int.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE, int.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.RECONNECT_ON_INIT, Boolean.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.RECONNECTION_POLICY_CLASS, String.class);
                putDurationMilliseconds(configurations, prefix, DefaultDriverOption.RECONNECTION_BASE_DELAY);
                putDurationMilliseconds(configurations, prefix, DefaultDriverOption.RECONNECTION_MAX_DELAY);
                putConfiguration(configurations, prefix, DefaultDriverOption.RETRY_POLICY_CLASS, String.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.SPECULATIVE_EXECUTION_POLICY_CLASS, String.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.SPECULATIVE_EXECUTION_MAX, int.class);
                putDurationMilliseconds(configurations, prefix, DefaultDriverOption.SPECULATIVE_EXECUTION_DELAY);
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
                putConfiguration(configurations, prefix, DefaultDriverOption.TIMESTAMP_GENERATOR_CLASS, String.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.TIMESTAMP_GENERATOR_FORCE_JAVA_CLOCK, Boolean.class);
                putDurationMilliseconds(configurations, prefix, DefaultDriverOption.TIMESTAMP_GENERATOR_DRIFT_WARNING_THRESHOLD);
                putDurationMilliseconds(configurations, prefix, DefaultDriverOption.TIMESTAMP_GENERATOR_DRIFT_WARNING_INTERVAL);
                putConfiguration(configurations, prefix, DefaultDriverOption.REQUEST_TRACKER_CLASS, String.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.REQUEST_LOGGER_SUCCESS_ENABLED, Boolean.class);
                putDurationMilliseconds(configurations, prefix, DefaultDriverOption.REQUEST_LOGGER_SLOW_THRESHOLD);
                putConfiguration(configurations, prefix, DefaultDriverOption.REQUEST_LOGGER_SLOW_ENABLED, Boolean.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.REQUEST_LOGGER_ERROR_ENABLED, Boolean.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.REQUEST_LOGGER_MAX_QUERY_LENGTH, int.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.REQUEST_LOGGER_VALUES, Boolean.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.REQUEST_LOGGER_MAX_VALUE_LENGTH, int.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.REQUEST_LOGGER_MAX_VALUES, int.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.REQUEST_LOGGER_STACK_TRACES, boolean.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.REQUEST_THROTTLER_CLASS, String.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.REQUEST_THROTTLER_MAX_CONCURRENT_REQUESTS, int.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.REQUEST_THROTTLER_MAX_REQUESTS_PER_SECOND, int.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.REQUEST_THROTTLER_MAX_QUEUE_SIZE, int.class);
                putDurationMilliseconds(configurations, prefix, DefaultDriverOption.REQUEST_THROTTLER_DRAIN_INTERVAL);
                putConfiguration(configurations, prefix, DefaultDriverOption.METADATA_NODE_STATE_LISTENER_CLASS, String.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.METADATA_SCHEMA_CHANGE_LISTENER_CLASS, String.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.ADDRESS_TRANSLATOR_CLASS, String.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.PROTOCOL_VERSION, String.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.PROTOCOL_COMPRESSION, String.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.PROTOCOL_MAX_FRAME_LENGTH, int.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.REQUEST_WARN_IF_SET_KEYSPACE, boolean.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.REQUEST_TRACE_ATTEMPTS, int.class);
                putDurationMilliseconds(configurations, prefix, DefaultDriverOption.REQUEST_TRACE_INTERVAL);
                putConfiguration(configurations, prefix, DefaultDriverOption.REQUEST_TRACE_CONSISTENCY, String.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.METRICS_SESSION_ENABLED, List.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.METRICS_NODE_ENABLED, List.class);

                putDurationMilliseconds(configurations, prefix, DefaultDriverOption.METRICS_SESSION_CQL_REQUESTS_HIGHEST);
                putConfiguration(configurations, prefix, DefaultDriverOption.METRICS_SESSION_CQL_REQUESTS_DIGITS, int.class);
                putDurationMilliseconds(configurations, prefix, DefaultDriverOption.METRICS_SESSION_CQL_REQUESTS_INTERVAL);

                putDurationMilliseconds(configurations, prefix, DefaultDriverOption.METRICS_SESSION_THROTTLING_HIGHEST);
                putConfiguration(configurations, prefix, DefaultDriverOption.METRICS_SESSION_THROTTLING_DIGITS, int.class);
                putDurationMilliseconds(configurations, prefix, DefaultDriverOption.METRICS_SESSION_THROTTLING_INTERVAL);

                putDurationMilliseconds(configurations, prefix, DefaultDriverOption.METRICS_NODE_CQL_MESSAGES_HIGHEST);
                putConfiguration(configurations, prefix, DefaultDriverOption.METRICS_NODE_CQL_MESSAGES_DIGITS, int.class);
                putDurationMilliseconds(configurations, prefix, DefaultDriverOption.METRICS_NODE_CQL_MESSAGES_INTERVAL);

                putConfiguration(configurations, prefix, DefaultDriverOption.SOCKET_TCP_NODELAY, Boolean.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.SOCKET_KEEP_ALIVE, Boolean.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.SOCKET_REUSE_ADDRESS, Boolean.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.SOCKET_LINGER_INTERVAL, int.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.SOCKET_RECEIVE_BUFFER_SIZE, int.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.SOCKET_SEND_BUFFER_SIZE, int.class);
                putDurationMilliseconds(configurations, prefix, DefaultDriverOption.HEARTBEAT_INTERVAL);
                putDurationMilliseconds(configurations, prefix, DefaultDriverOption.HEARTBEAT_TIMEOUT);
                putDurationMilliseconds(configurations, prefix, DefaultDriverOption.METADATA_TOPOLOGY_WINDOW);
                putConfiguration(configurations, prefix, DefaultDriverOption.METADATA_TOPOLOGY_MAX_EVENTS, int.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.METADATA_SCHEMA_ENABLED, boolean.class);
                putDurationMilliseconds(configurations, prefix, DefaultDriverOption.METADATA_SCHEMA_REQUEST_TIMEOUT);

                putConfiguration(configurations, prefix, DefaultDriverOption.METADATA_SCHEMA_REQUEST_PAGE_SIZE, List.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.METADATA_SCHEMA_REFRESHED_KEYSPACES, List.class);
                putDurationMilliseconds(configurations, prefix, DefaultDriverOption.METADATA_SCHEMA_WINDOW);
                putConfiguration(configurations, prefix, DefaultDriverOption.METADATA_SCHEMA_MAX_EVENTS, int.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.METADATA_TOKEN_MAP_ENABLED, Boolean.class);
                putDurationMilliseconds(configurations, prefix, DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT);
                putDurationMilliseconds(configurations, prefix, DefaultDriverOption.CONTROL_CONNECTION_AGREEMENT_INTERVAL);
                putDurationMilliseconds(configurations, prefix, DefaultDriverOption.CONTROL_CONNECTION_AGREEMENT_TIMEOUT);
                putConfiguration(configurations, prefix, DefaultDriverOption.CONTROL_CONNECTION_AGREEMENT_WARN, boolean.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.PREPARE_ON_ALL_NODES, boolean.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.REPREPARE_ENABLED, boolean.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.REPREPARE_CHECK_SYSTEM_TABLE, boolean.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.REPREPARE_MAX_STATEMENTS, int.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.REPREPARE_MAX_PARALLELISM, int.class);
                putDurationMilliseconds(configurations, prefix, DefaultDriverOption.REPREPARE_TIMEOUT);
                putConfiguration(configurations, prefix, DefaultDriverOption.NETTY_IO_SIZE, int.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.NETTY_IO_SHUTDOWN_QUIET_PERIOD, int.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.NETTY_IO_SHUTDOWN_TIMEOUT, int.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.NETTY_IO_SHUTDOWN_UNIT, String.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.NETTY_ADMIN_SIZE, int.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.NETTY_ADMIN_SHUTDOWN_QUIET_PERIOD, int.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.NETTY_ADMIN_SHUTDOWN_TIMEOUT, int.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.NETTY_ADMIN_SHUTDOWN_UNIT, String.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.COALESCER_MAX_RUNS, int.class);
                putDurationNanoSeconds(configurations, prefix, DefaultDriverOption.COALESCER_INTERVAL);
                putConfiguration(configurations, prefix, DefaultDriverOption.RESOLVE_CONTACT_POINTS, boolean.class);
                putDurationMilliseconds(configurations, prefix, DefaultDriverOption.NETTY_TIMER_TICK_DURATION);
                putConfiguration(configurations, prefix, DefaultDriverOption.NETTY_TIMER_TICKS_PER_WHEEL, int.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.REQUEST_LOG_WARNINGS, boolean.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.NETTY_DAEMON, boolean.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.CLOUD_SECURE_CONNECT_BUNDLE, String.class);
                putConfiguration(configurations, prefix, DefaultDriverOption.LOAD_BALANCING_POLICY_SLOW_AVOIDANCE, boolean.class);

                ConfigFactory.invalidateCaches();
                return ConfigFactory.parseMap(configurations).withFallback(ConfigFactory.load().getConfig(DefaultDriverConfigLoader.DEFAULT_ROOT_PATH));
            }));
            return builder;
        } catch (Exception e) {
            LOG.error("Failed to insantiate CQL session: " + e.getMessage(), e);
            throw e;
        }

    }

    /**
     * Creates the {@link CqlSession} bean for the given configuration.
     *
     * @param builder The {@link CqlSessionBuilder}
     * @return A {@link CqlSession} bean
     */
    @EachBean(CqlSessionBuilder.class)
    @Bean(preDestroy = "close")
    public CqlSession cassandraCluster(CqlSessionBuilder builder) {
        CqlSession session = builder.build();
        this.sessions.add(session);
        return session;
    }

    /**
     * closes all active {@link CqlSession}.
     */
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
