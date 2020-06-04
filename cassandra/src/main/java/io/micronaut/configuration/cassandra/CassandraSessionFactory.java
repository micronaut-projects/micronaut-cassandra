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
package io.micronaut.configuration.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader;
import com.typesafe.config.ConfigFactory;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.EachBean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.core.naming.conventions.StringConvention;
import io.micronaut.core.value.PropertyResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;

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
     * Creates the {@link CqlSessionBuilder} bean for the given configuration.
     *
     * @param configuration The cassandra configuration bean
     * @return A {@link CqlSession} bean
     */
    @EachBean(CassandraConfiguration.class)
    public CqlSessionBuilder session(CassandraConfiguration configuration) {

        try {
            CqlSessionBuilder builder = CqlSession.builder().withConfigLoader(new DefaultDriverConfigLoader(() -> {
                ConfigFactory.invalidateCaches();
                String prefix = configuration.getName();
                return ConfigFactory.parseMap(this.resolver.getProperties(CassandraConfiguration.PREFIX + "." + prefix, StringConvention.RAW)).withFallback(ConfigFactory.load().getConfig(DefaultDriverConfigLoader.DEFAULT_ROOT_PATH));
            }));
            return builder;
        } catch (Exception e) {
            LOG.error("Failed to instantiate CQL session: " + e.getMessage(), e);
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
