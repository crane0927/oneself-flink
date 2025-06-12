package com.oneself.common.cache.redis.impl;

import com.oneself.common.cache.redis.RedisOps;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.params.SetParams;

import java.util.*;

/**
 * @author liuhuan
 * date 2025/5/23
 * packageName com.oneself.common.cache.redis.impl
 * className JedisPoolOps
 * description
 * version 1.0
 */
public class JedisPoolOps implements RedisOps {

    private final JedisPool jedisPool;

    public JedisPoolOps(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    @Override
    public String get(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.get(key);
        }
    }

    @Override
    public String set(String key, String value) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.set(key, value);
        }
    }

    @Override
    public Boolean exists(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.exists(key);
        }
    }

    @Override
    public Long del(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.del(key);
        }
    }

    @Override
    public Long expire(String key, int seconds) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.expire(key, seconds);
        }
    }

    @Override
    public Long ttl(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.ttl(key);
        }
    }

    @Override
    public Map<String, String> hGetAll(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.hgetAll(key);
        }
    }

    @Override
    public String hmSet(String key, Map<String, String> values) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.hmset(key, values);
        }
    }

    @Override
    public Long hSet(String key, String field, String value) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.hset(key, field, value);
        }
    }

    @Override
    public Long hDel(String key, String... fields) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.hdel(key, fields);
        }
    }

    @Override
    public String hGet(String key, String field) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.hget(key, field);
        }
    }

    @Override
    public Set<String> sMembers(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.smembers(key);
        }
    }

    @Override
    public Long sAdd(String key, String... members) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.sadd(key, members);
        }
    }

    @Override
    public Long sRem(String key, String... members) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.srem(key, members);
        }
    }

    @Override
    public Long lPush(String key, String... values) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.lpush(key, values);
        }
    }

    @Override
    public String rPop(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.rpop(key);
        }
    }

    @Override
    public List<String> lRange(String key, long start, long stop) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.lrange(key, start, stop);
        }
    }

    @Override
    public void mSet(Map<String, String> keyValueMap) {
        try (Jedis jedis = jedisPool.getResource()) {
            String[] kvPairs = keyValueMap.entrySet().stream()
                    .flatMap(e -> Arrays.stream(new String[]{e.getKey(), e.getValue()}))
                    .toArray(String[]::new);
            jedis.mset(kvPairs);
        }
    }

    @Override
    public Map<String, String> mGet(String... keys) {
        try (Jedis jedis = jedisPool.getResource()) {
            List<String> values = jedis.mget(keys);
            Map<String, String> result = new HashMap<>();
            for (int i = 0; i < keys.length; i++) {
                result.put(keys[i], values.get(i));
            }
            return result;
        }
    }

    @Override
    public boolean tryLock(String lockKey, String requestId, int seconds) {
        try (Jedis jedis = jedisPool.getResource()) {
            SetParams params = new SetParams().nx().ex(seconds);
            String result = jedis.set(lockKey, requestId, params);
            return "OK".equals(result);
        }
    }

    @Override
    public boolean releaseLock(String lockKey, String requestId) {
        String lua = "if redis.call('get', KEYS[1]) == ARGV[1] then " +
                "return redis.call('del', KEYS[1]) else return 0 end";
        try (Jedis jedis = jedisPool.getResource()) {
            Object result = jedis.eval(lua, Collections.singletonList(lockKey), Collections.singletonList(requestId));
            return Long.valueOf(1L).equals(result);
        }
    }
}
