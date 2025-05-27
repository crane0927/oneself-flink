package com.oneself.ops;

import com.oneself.ops.impl.JedisClusterOps;
import com.oneself.ops.impl.JedisPoolOps;
import com.oneself.properties.RedisProperties;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.*;

import java.time.Duration;

/**
 * @author liuhuan
 * date 2025/5/23
 * packageName com.oneself.ops
 * className RedisOpsFactory
 * description
 * version 1.0
 */
public class RedisOpsFactory {

    public static RedisOps create(RedisProperties properties) {
        return "CLUSTER".equals(properties.isClusterMode())
                ? initJedisCluster(properties)
                : initJedisPool(properties);
    }

    private static RedisOps initJedisPool(RedisProperties props) {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(props.getMaxTotal());
        poolConfig.setMaxIdle(props.getMaxIdle());
        poolConfig.setMaxWait(Duration.ofMillis(props.getMaxWaitMillis()));
        poolConfig.setTestOnBorrow(props.isTestOnBorrow());

        HostAndPort hostAndPort = props.getPoolHostAndPort();

        return new JedisPoolOps(
                new JedisPool(
                        poolConfig,
                        hostAndPort.getHost(),
                        hostAndPort.getPort(),
                        props.getConnectionTimeout(),
                        props.getAuth(),
                        props.getDatabase()
                )
        );
    }

    private static RedisOps initJedisCluster(RedisProperties props) {
        GenericObjectPoolConfig<Connection> poolConfig = new GenericObjectPoolConfig<>();
        poolConfig.setMaxTotal(props.getMaxTotal());
        poolConfig.setMaxIdle(props.getMaxIdle());
        poolConfig.setMaxWait(Duration.ofMillis(props.getMaxWaitMillis()));
        poolConfig.setTestOnBorrow(props.isTestOnBorrow());

        JedisCluster jedisCluster = new JedisCluster(
                props.getClusterAddresses(),
                props.getConnectionTimeout(),
                props.getClusterSoTimeout(),
                props.getClusterMaxAttempts(),
                props.getAuth(),
                poolConfig
        );
        return new JedisClusterOps(jedisCluster);
    }
}
