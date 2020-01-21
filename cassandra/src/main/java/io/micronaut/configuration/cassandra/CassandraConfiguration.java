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

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.context.annotation.EachProperty;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Allows the configuration of a Cassandra Cluster connection using the datastax driver.
 *
 * The client is able to be configured to multiple clusters. If there are multiple configuration keys, default can be
 * used to denote the primary cluster bean.
 *
 * @author Nirav Assar
 * @since 1.0
 */
@EachProperty(value = CassandraConfiguration.PREFIX, primary = "default")
public class CassandraConfiguration{
    public static final String PREFIX = "cassandra.datasource";

    private String name = "";
    private String keyspace = null;
    private RequestConfigurationProperties request = new RequestConfigurationProperties();
    private LoadBalancerPolicyProperties loadBalancerPolicy = new LoadBalancerPolicyProperties();

    public LoadBalancerPolicyProperties getLoadBalancerPolicy() {
        return loadBalancerPolicy;
    }

    public void setLoadBalancerPolicy(LoadBalancerPolicyProperties loadBalancerPolicy) {
        this.loadBalancerPolicy = loadBalancerPolicy;
    }

    private List<String> contactPoints;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setContactPoints(List<String> contactPoints) {
        this.contactPoints = contactPoints;
    }

    public List<String> getContactPoints() {
        return contactPoints;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }

    public RequestConfigurationProperties getRequest() {
        return request;
    }

    public void setRequest(RequestConfigurationProperties request) {
        this.request = request;
    }

    @ConfigurationProperties("load-balancing-policy")
    public static class LoadBalancerPolicyProperties {
        private String policyClass;
        private String localDatacenter;
        private String filterClass;

        public String getPolicyClass() {
            return policyClass;
        }

        public void setPolicyClass(String policyClass) {
            this.policyClass = policyClass;
        }

        public void setLocalDatacenter(String localDatacenter) {
            this.localDatacenter = localDatacenter;
        }

        public String getLocalDatacenter() {
            return localDatacenter;
        }

        public String getFilterClass() {
            return filterClass;
        }

        public void setFilterClass(String filterClass) {
            this.filterClass = filterClass;
        }
    }

    @ConfigurationProperties("request")
    public static class RequestConfigurationProperties {
        private int timeout = 2000;
        private DefaultConsistencyLevel consistency = DefaultConsistencyLevel.LOCAL_ONE;
        private int pageSize =  5000;
        private ConsistencyLevel consistencyLevel = ConsistencyLevel.SERIAL;
        private boolean defaultIdempotency = false;

        public void setDefaultIdempotency(boolean defaultIdempotency) {
            this.defaultIdempotency = defaultIdempotency;
        }

        public boolean isDefaultIdempotency() {
            return defaultIdempotency;
        }

        public void setPageSize(int pageSize) {
            this.pageSize = pageSize;
        }

        public int getPageSize() {
            return pageSize;
        }

        public void setConsistencyLevel(ConsistencyLevel consistencyLevel) {
            this.consistencyLevel = consistencyLevel;
        }

        public ConsistencyLevel getConsistencyLevel() {
            return consistencyLevel;
        }

        public int getTimeout() {
            return timeout;
        }

        public void setTimeout(int timeout) {
            this.timeout = timeout;
        }

        public DefaultConsistencyLevel getConsistency() {
            return consistency;
        }

        public void setConsistency(DefaultConsistencyLevel consistency) {
            this.consistency = consistency;
        }
    }



}
