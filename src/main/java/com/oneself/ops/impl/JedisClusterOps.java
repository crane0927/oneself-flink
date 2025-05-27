package com.oneself.ops.impl;

import com.oneself.ops.RedisOps;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.params.SetParams;

import java.util.*;

/**
 * @author liuhuan
 * date 2025/5/23
 * packageName com.oneself.ops.impl
 * className JedisClusterOps
 * description
 * version 1.0
 */
public class JedisClusterOps implements RedisOps {

    private final JedisCluster jedisCluster;

    public JedisClusterOps(JedisCluster jedisCluster) {
        this.jedisCluster = jedisCluster;
    }

    @Override
    public String get(String key) {
        return jedisCluster.get(key);
    }

    @Override
    public String set(String key, String value) {
        return jedisCluster.set(key, value);
    }

    @Override
    public Boolean exists(String key) {
        return jedisCluster.exists(key);
    }

    @Override
    public Long del(String key) {
        return jedisCluster.del(key);
    }

    @Override
    public Long expire(String key, int seconds) {
        return jedisCluster.expire(key, seconds);
    }

    @Override
    public Long ttl(String key) {
        return jedisCluster.ttl(key);
    }

    @Override
    public Map<String, String> hGetAll(String key) {
        return jedisCluster.hgetAll(key);
    }

    @Override
    public String hmSet(String key, Map<String, String> values) {
        return jedisCluster.hmset(key, values);
    }

    @Override
    public Long hSet(String key, String field, String value) {
        return jedisCluster.hset(key, field, value);
    }

    @Override
    public Long hDel(String key, String... fields) {
        return jedisCluster.hdel(key, fields);
    }

    @Override
    public String hGet(String key, String field) {
        return jedisCluster.hget(key, field);
    }

    @Override
    public Set<String> sMembers(String key) {
        return jedisCluster.smembers(key);
    }

    @Override
    public Long sAdd(String key, String... members) {
        return jedisCluster.sadd(key, members);
    }

    @Override
    public Long sRem(String key, String... members) {
        return jedisCluster.srem(key, members);
    }

    @Override
    public Long lPush(String key, String... values) {
        return jedisCluster.lpush(key, values);
    }

    @Override
    public String rPop(String key) {
        return jedisCluster.rpop(key);
    }

    @Override
    public List<String> lRange(String key, long start, long stop) {
        return jedisCluster.lrange(key, start, stop);
    }

    @Override
    public void mSet(Map<String, String> keyValueMap) {
        // JedisCluster 不支持 mset 跨 slot，因此循环设置
        for (Map.Entry<String, String> entry : keyValueMap.entrySet()) {
            jedisCluster.set(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public Map<String, String> mGet(String... keys) {
        // JedisCluster 不支持 mget 跨 slot，因此循环获取
        Map<String, String> result = new HashMap<>();
        for (String key : keys) {
            String value = jedisCluster.get(key);
            result.put(key, value);
        }
        return result;
    }

    @Override
    public boolean tryLock(String lockKey, String requestId, int seconds) {
        SetParams params = new SetParams().nx().ex(seconds);
        String result = jedisCluster.set(lockKey, requestId, params);
        return "OK".equals(result);
    }

    @Override
    public boolean releaseLock(String lockKey, String requestId) {
        String script = "if redis.call('get',KEYS[1]) == ARGV[1] then return redis.call('del',KEYS[1]) else return 0 end";
        Object result = jedisCluster.eval(script, Collections.singletonList(lockKey), Collections.singletonList(requestId));
        return Long.valueOf(1L).equals(result);
    }
}
