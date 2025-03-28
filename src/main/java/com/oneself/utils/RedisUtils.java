package com.oneself.utils;

/**
 * @author liuhuan
 * date 2025/3/3
 * packageName com.oneself.utils
 * className RedisUtils
 * description
 * version 1.0
 */

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.params.SetParams;
import redis.clients.jedis.exceptions.JedisException;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.time.Duration;

/**
 * Redis工具类，提供Redis操作的封装
 */
public class RedisUtils {

    private static final Logger log = LoggerFactory.getLogger(RedisUtils.class);
    private static final int MAX_RETRY_TIMES = 3;
    private static final long RETRY_DELAY_MS = 1000;
    private static final JedisPool jedisPool;

    // 静态代码块初始化 JedisPool
    static {
        // 配置 Jedis 连接池
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(128); // 设置最大连接数
        config.setMaxIdle(64);   // 设置最大空闲连接数
        config.setMinIdle(16);   // 设置最小空闲连接数
        config.setTestOnBorrow(true); // 测试连接可用性
        config.setTestOnReturn(true);
        config.setTestWhileIdle(true);
        config.setJmxEnabled(true); // 启用JMX监控
        config.setMaxWait(Duration.ofMillis(2000)); // 设置最大等待时间
        config.setMinEvictableIdleTime(Duration.ofMillis(30000)); // 设置空闲连接最小空闲时间
        config.setTimeBetweenEvictionRuns(Duration.ofMillis(30000)); // 设置空闲连接检测间隔
        config.setNumTestsPerEvictionRun(3);

        // 连接池初始化，Redis 主机地址和端口
        String redisPassword = "mBYFyN4wXi6WsA"; // 替换为你的 Redis 密码

        // 连接池初始化，Redis 主机地址、端口和密码
        jedisPool = new JedisPool(config, "localhost", 6379, 2000, redisPassword);
    }

    /**
     * 执行Redis操作，带重试机制
     *
     * @param operation Redis操作函数
     * @param <T>       返回值类型
     * @return 操作结果
     */
    private static <T> T executeWithRetry(RedisOperation<T> operation) {
        int retryCount = 0;
        Exception lastException = null;

        while (retryCount < MAX_RETRY_TIMES) {
            try (Jedis jedis = jedisPool.getResource()) {
                return operation.execute(jedis);
            } catch (JedisException e) {
                lastException = e;
                retryCount++;
                if (retryCount < MAX_RETRY_TIMES) {
                    log.warn("Redis operation failed, retrying {}/{} times. Error: {}",
                            retryCount, MAX_RETRY_TIMES, e.getMessage());
                    try {
                        TimeUnit.MILLISECONDS.sleep(RETRY_DELAY_MS);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }

        log.error("Redis operation failed after {} retries", MAX_RETRY_TIMES, lastException);
        throw new RuntimeException("Redis operation failed", lastException);
    }

    // 获取 Jedis 实例
    private static Jedis getJedis() {
        return jedisPool.getResource();
    }

    // 关闭 Jedis 连接
    private static void closeJedis(Jedis jedis) {
        if (jedis != null) {
            jedis.close();
        }
    }

    /**
     * 获取Redis中的字符串值
     *
     * @param key 键
     * @return 值
     */
    public static String getString(String key) {
        return executeWithRetry(jedis -> jedis.get(key));
    }

    /**
     * 设置Redis中的字符串值
     *
     * @param key   键
     * @param value 值
     * @return 操作结果
     */
    public static String setString(String key, String value) {
        return executeWithRetry(jedis -> jedis.set(key, value));
    }

    /**
     * 设置Redis中的字符串值，带过期时间
     *
     * @param key     键
     * @param value   值
     * @param seconds 过期时间（秒）
     * @return 操作结果
     */
    public static String setStringWithExpire(String key, String value, long seconds) {
        return executeWithRetry(jedis -> jedis.setex(key, seconds, value));
    }

    /**
     * 设置Redis中的字符串值，带参数
     *
     * @param key    键
     * @param value  值
     * @param params 设置参数
     * @return 操作结果
     */
    public static String setStringWithParams(String key, String value, SetParams params) {
        return executeWithRetry(jedis -> jedis.set(key, value, params));
    }

    /**
     * 删除Redis中的键
     *
     * @param key 键
     * @return 删除的键数量
     */
    public static Long deleteKey(String key) {
        return executeWithRetry(jedis -> jedis.del(key));
    }

    /**
     * 批量删除Redis中的键
     *
     * @param keys 键数组
     * @return 删除的键数量
     */
    public static Long deleteKeys(String... keys) {
        return executeWithRetry(jedis -> jedis.del(keys));
    }

    /**
     * 检查键是否存在
     *
     * @param key 键
     * @return 是否存在
     */
    public static Boolean isKeyExists(String key) {
        return executeWithRetry(jedis -> jedis.exists(key));
    }

    /**
     * 设置键的过期时间
     *
     * @param key     键
     * @param seconds 过期时间（秒）
     * @return 是否设置成功
     */
    public static Long setExpireTime(String key, int seconds) {
        return executeWithRetry(jedis -> jedis.expire(key, seconds));
    }

    /**
     * 获取键的剩余过期时间
     *
     * @param key 键
     * @return 剩余过期时间（秒）
     */
    public static Long getTimeToLive(String key) {
        return executeWithRetry(jedis -> jedis.ttl(key));
    }

    /**
     * 获取哈希表中的所有字段和值
     *
     * @param hashKey 哈希表键
     * @return 字段和值的映射
     */
    public static Map<String, String> getHashAll(String hashKey) {
        return executeWithRetry(jedis -> jedis.hgetAll(hashKey));
    }

    /**
     * 获取哈希表中指定字段的值
     *
     * @param hashKey 哈希表键
     * @param field   字段
     * @return 字段值
     */
    public static String getHashField(String hashKey, String field) {
        return executeWithRetry(jedis -> jedis.hget(hashKey, field));
    }

    /**
     * 设置哈希表中指定字段的值
     *
     * @param hashKey 哈希表键
     * @param field   字段
     * @param value   值
     * @return 操作结果
     */
    public static Long setHashField(String hashKey, String field, String value) {
        return executeWithRetry(jedis -> jedis.hset(hashKey, field, value));
    }

    /**
     * 删除哈希表中的指定字段
     *
     * @param hashKey 哈希表键
     * @param fields  字段数组
     * @return 删除的字段数量
     */
    public static Long deleteHashFields(String hashKey, String... fields) {
        return executeWithRetry(jedis -> jedis.hdel(hashKey, fields));
    }

    /**
     * 获取Set中的所有成员
     *
     * @param key Set键
     * @return 成员集合
     */
    public static Set<String> getSetMembers(String key) {
        return executeWithRetry(jedis -> jedis.smembers(key));
    }

    /**
     * 向Set中添加成员
     *
     * @param key     Set键
     * @param members 成员数组
     * @return 添加的成员数量
     */
    public static Long addSetMembers(String key, String... members) {
        return executeWithRetry(jedis -> jedis.sadd(key, members));
    }

    /**
     * 从Set中删除成员
     *
     * @param key     Set键
     * @param members 成员数组
     * @return 删除的成员数量
     */
    public static Long removeSetMembers(String key, String... members) {
        return executeWithRetry(jedis -> jedis.srem(key, members));
    }

    /**
     * 向List头部添加元素
     *
     * @param key    List键
     * @param values 值数组
     * @return 操作后的List长度
     */
    public static Long addToListHead(String key, String... values) {
        return executeWithRetry(jedis -> jedis.lpush(key, values));
    }

    /**
     * 从List尾部弹出元素
     *
     * @param key List键
     * @return 弹出的元素
     */
    public static String popFromListTail(String key) {
        return executeWithRetry(jedis -> jedis.rpop(key));
    }

    /**
     * 获取List指定范围的元素
     *
     * @param key   List键
     * @param start 起始位置
     * @param stop  结束位置
     * @return 元素列表
     */
    public static List<String> getListRange(String key, long start, long stop) {
        return executeWithRetry(jedis -> jedis.lrange(key, start, stop));
    }

    /**
     * 尝试获取分布式锁
     *
     * @param lockKey   锁键
     * @param requestId 请求ID
     * @param seconds   过期时间（秒）
     * @return 是否获取成功
     */
    public static boolean tryDistributedLock(String lockKey, String requestId, int seconds) {
        SetParams params = new SetParams();
        params.nx().ex(seconds);
        String result = setStringWithParams(lockKey, requestId, params);
        return "OK".equals(result);
    }

    /**
     * 释放分布式锁
     *
     * @param lockKey   锁键
     * @param requestId 请求ID
     * @return 是否释放成功
     */
    public static boolean releaseDistributedLock(String lockKey, String requestId) {
        String script = "if redis.call('get',KEYS[1]) == ARGV[1] then " +
                "return redis.call('del',KEYS[1]) else return 0 end";
        Long result = executeWithRetry(jedis ->
                (Long) jedis.eval(script, 1, lockKey, requestId));
        return result != null && result == 1;
    }

    /**
     * 批量设置键值对
     *
     * @param keyValueMap 键值对映射
     */
    public static void setBatch(Map<String, String> keyValueMap) {
        executeWithRetry(jedis -> {
            jedis.mset(keyValueMap.entrySet().stream()
                    .map(e -> new String[]{e.getKey(), e.getValue()})
                    .flatMap(arr -> java.util.Arrays.stream(arr))
                    .toArray(String[]::new));
            return null;
        });
    }

    /**
     * 批量获取键值对
     *
     * @param keys 键数组
     * @return 键值对映射
     */
    public static Map<String, String> getBatch(String... keys) {
        List<String> values = executeWithRetry(jedis -> jedis.mget(keys));
        Map<String, String> result = new java.util.HashMap<>();
        for (int i = 0; i < keys.length; i++) {
            result.put(keys[i], values.get(i));
        }
        return result;
    }

    /**
     * 关闭Redis连接池
     */
    public static void closePool() {
        if (jedisPool != null) {
            jedisPool.close();
        }
    }

    // Redis操作接口
    @FunctionalInterface
    private interface RedisOperation<T> {
        T execute(Jedis jedis);
    }
}