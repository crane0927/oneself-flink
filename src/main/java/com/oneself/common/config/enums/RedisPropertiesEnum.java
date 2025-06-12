package com.oneself.common.config.enums;

/**
 * @author liuhuan
 * date 2025/5/23
 * packageName com.oneself.common.config.enums
 * enumName RedisPropertiesEnum
 * description Redis 配置信息枚举 映射 onesel-flink.properties 中的 Redis 相关配置信息
 * version 1.0
 */
public enum RedisPropertiesEnum {

    MODE("redis.mode", "Redis 模式"),
    // 公共通用配置
    AUTH("redis.config.auth", "Redis 认证密码"),
    MAX_TOTAL("redis.config.maxTotal", "最大连接数"),
    MAX_IDLE("redis.config.maxIdle", "最大空闲连接数"),
    MAX_WAIT_MILLIS("redis.config.maxWaitMillis", "最大等待时间（毫秒）"),
    TEST_ON_BORROW("redis.config.testOnBorrow", "借用连接时测试"),
    CONNECTION_TIMEOUT("redis.config.connectionTimeout", "连接超时时间（毫秒）"),

    // 单机配置
    POOL_ADDRESS("redis.pool.config.addr", "Redis 单节点地址"),
    POOL_DATABASE("redis.pool.config.database", "Redis 单节点数据库索引"),

    // 集群配置
    CLUSTER_ADDRESS("redis.cluster.config.addr", "Redis 集群地址"),
    CLUSTER_SO_TIMEOUT("redis.cluster.config.soTimeout", "集群 socket 超时（毫秒）"),
    CLUSTER_MAX_ATTEMPTS("redis.cluster.config.maxAttempts", "集群最大重试次数");

    private final String key;
    private final String description;

    RedisPropertiesEnum(String key, String description) {
        this.key = key;
        this.description = description;
    }

    public String getKey() {
        return key;
    }

    public String getDescription() {
        return description;
    }
}
