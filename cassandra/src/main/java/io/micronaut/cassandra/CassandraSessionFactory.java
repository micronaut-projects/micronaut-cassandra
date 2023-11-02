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
package io.micronaut.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader;
import com.typesafe.config.ConfigFactory;
import io.micronaut.context.annotation.EachBean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.core.value.PropertyResolver;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Creates cassandra cluster for each configuration bean.
 *
 * @author Nirav Assar
 * @author Michael Pollind
 * @author Dean Wette
 * @since 1.0
 */
@Factory
public class CassandraSessionFactory implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraSessionFactory.class);
    private final List<CqlSession> sessions = new ArrayList<>(2);
    private final PropertyResolver resolver;

    /**
     * Default constructor.
     *
     * @param propertyResolver Property resolve for application configurations
     */
    public CassandraSessionFactory(PropertyResolver propertyResolver) {
        this.resolver = propertyResolver;
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
            return CqlSession.builder().withConfigLoader(new DefaultDriverConfigLoader(() -> {
                ConfigFactory.invalidateCaches();
                String prefix = configuration.getName();
                Map<String, Object> properties = this.resolver.getProperties(CassandraConfiguration.PREFIX + "." + prefix);
                // translate indexed properties for list values from Micronaut array index notation (i.e. foo[0]=bar)
                // to Datastax driver decimal notation (i.e. foo.0=bar)
                properties = properties.entrySet().stream()
                    .filter(e -> !(e.getValue() instanceof Collection<?>))
                    .collect((Collectors.toMap(e -> e.getKey().replaceAll("\\[(\\d+)]", ".$1"), Map.Entry::getValue)));
                return ConfigFactory.parseMap(properties).withFallback(ConfigFactory.load().getConfig(DefaultDriverConfigLoader.DEFAULT_ROOT_PATH));
            }));
        } catch (Exception e) {
            LOG.error(String.format("Failed to instantiate CQL session: %s", e.getMessage()), e);
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
        for (CqlSession session : sessions) {
            try {
                session.close();
            } catch (Exception e) {
                if (LOG.isWarnEnabled()) {
                    LOG.warn(String.format("Error closing data source [%s]: %s", session, e.getMessage()), e);
                }
            }
        }
    }
}
