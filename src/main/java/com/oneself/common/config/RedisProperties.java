package com.oneself.common.config;

import com.oneself.common.config.enums.RedisPropertiesEnum;
import org.apache.flink.api.java.utils.ParameterTool;
import redis.clients.jedis.HostAndPort;

import java.util.HashSet;
import java.util.Set;

/**
 * @author liuhuan
 * date 2025/5/23
 * packageName com.oneself.common.config
 * className RedisProperties
 * description
 * version 1.0
 */
public class RedisProperties {

    private final ParameterTool properties;

    public RedisProperties(ParameterTool properties) {
        this.properties = properties;
    }

    public String isClusterMode() {
        return properties.get(RedisPropertiesEnum.MODE.getKey());
    }

    public String getClusterAddress() {
        return properties.get(RedisPropertiesEnum.CLUSTER_ADDRESS.getKey());
    }

    public String getPoolAddress() {
        return properties.get(RedisPropertiesEnum.POOL_ADDRESS.getKey());
    }

    public int getMaxTotal() {
        return Integer.parseInt(properties.get(RedisPropertiesEnum.MAX_TOTAL.getKey(), "10"));
    }

    public int getMaxIdle() {
        return Integer.parseInt(properties.get(RedisPropertiesEnum.MAX_IDLE.getKey(), "5"));
    }

    public long getMaxWaitMillis() {
        return Long.parseLong(properties.get(RedisPropertiesEnum.MAX_WAIT_MILLIS.getKey(), "2000"));
    }

    public boolean isTestOnBorrow() {
        return Boolean.parseBoolean(properties.get(RedisPropertiesEnum.TEST_ON_BORROW.getKey(), "true"));
    }

    public int getConnectionTimeout() {
        return Integer.parseInt(properties.get(RedisPropertiesEnum.CONNECTION_TIMEOUT.getKey(), "3000"));
    }

    public String getAuth() {
        return properties.get(RedisPropertiesEnum.AUTH.getKey(), null);
    }

    public int getClusterSoTimeout() {
        return Integer.parseInt(properties.get(RedisPropertiesEnum.CLUSTER_SO_TIMEOUT.getKey(), "3000"));
    }

    public int getClusterMaxAttempts() {
        return Integer.parseInt(properties.get(RedisPropertiesEnum.CLUSTER_MAX_ATTEMPTS.getKey(), "3"));
    }

    public int getDatabase() {
        return Integer.parseInt(properties.get(RedisPropertiesEnum.POOL_DATABASE.getKey(), "0"));
    }

    public HostAndPort getPoolHostAndPort() {
        String[] parts = getPoolAddress().split(":");
        return new HostAndPort(parts[0], Integer.parseInt(parts[1]));
    }

    public Set<HostAndPort> getClusterAddresses() {
        String[] addresses = getClusterAddress().split(",");
        Set<HostAndPort> nodes = new HashSet<>();
        for (String addr : addresses) {
            String[] parts = addr.trim().split(":");
            nodes.add(new HostAndPort(parts[0], Integer.parseInt(parts[1])));
        }
        return nodes;
    }
}
