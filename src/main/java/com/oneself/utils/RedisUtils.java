package com.oneself.utils;

/**
 * @author liuhuan
 * date 2025/3/3
 * packageName com.oneself.utils
 * className RedisUtils
 * description
 * version 1.0
 */

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Map;
import java.util.Set;

public class RedisUtils {

    private static JedisPool jedisPool;

    // 静态代码块初始化 JedisPool
    static {
        // 配置 Jedis 连接池
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(128); // 设置最大连接数
        config.setMaxIdle(64);   // 设置最大空闲连接数
        config.setMinIdle(16);   // 设置最小空闲连接数
        config.setTestOnBorrow(true); // 测试连接可用性

        // 连接池初始化，Redis 主机地址和端口
        String redisPassword = "mBYFyN4wXi6WsA"; // 替换为你的 Redis 密码

        // 连接池初始化，Redis 主机地址、端口和密码
        jedisPool = new JedisPool(config, "localhost", 6379, 2000, redisPassword); // 根据需要修改 host 和 port

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

    // 查询 Redis 字符串
    public static String get(String key) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.get(key); // 查询并返回
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        } finally {
            closeJedis(jedis);
        }
    }

    // 设置 Redis 字符串
    public static String set(String key, String value) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.set(key, value); // 设置并返回状态
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        } finally {
            closeJedis(jedis);
        }
    }

    // 删除 Redis 中的键
    public static Long del(String key) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.del(key); // 删除键
        } catch (Exception e) {
            e.printStackTrace();
            return 0L;
        } finally {
            closeJedis(jedis);
        }
    }

    // 查询 Redis 是否包含某个键
    public static Boolean exists(String key) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.exists(key); // 返回键是否存在
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            closeJedis(jedis);
        }
    }

    // 查询 Redis 哈希表中的所有字段和值
    public static Map<String, String> hgetAll(String hashKey) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.hgetAll(hashKey); // 获取哈希表中的所有字段和值
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        } finally {
            closeJedis(jedis);
        }
    }

    // 获取 Redis 中 Set 类型的数据
    public static Set<String> getSet(String key) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.smembers(key); // 获取 Set 中的所有成员
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        } finally {
            closeJedis(jedis);
        }
    }


}