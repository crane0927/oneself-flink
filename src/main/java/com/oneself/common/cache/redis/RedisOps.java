package com.oneself.common.cache.redis;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author liuhuan
 * date 2025/5/23
 * packageName com.oneself.common.cache.redis
 * interfaceName RedisOps
 * description
 * version 1.0
 */
public interface RedisOps {

    // 基础操作
    String get(String key);

    String set(String key, String value);

    Boolean exists(String key);

    Long del(String key);

    Long expire(String key, int seconds);

    Long ttl(String key);

    // Hash 操作
    Map<String, String> hGetAll(String key);

    String hmSet(String key, Map<String, String> values);

    Long hSet(String key, String field, String value);

    Long hDel(String key, String... fields);

    String hGet(String key, String field);

    // Set 操作
    Set<String> sMembers(String key);

    Long sAdd(String key, String... members);

    Long sRem(String key, String... members);

    // List 操作
    Long lPush(String key, String... values);

    String rPop(String key);

    List<String> lRange(String key, long start, long stop);

    // 批处理
    void mSet(Map<String, String> keyValueMap);

    Map<String, String> mGet(String... keys);

    // 分布式锁
    boolean tryLock(String lockKey, String requestId, int seconds);

    boolean releaseLock(String lockKey, String requestId);
}
