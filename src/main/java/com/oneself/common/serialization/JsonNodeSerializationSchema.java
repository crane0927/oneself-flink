package com.oneself.common.serialization;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.api.common.serialization.SerializationSchema;

/**
 * @author liuhuan
 * date 2025/3/13
 * packageName com.oneself.common.serialization
 * className JsonNodeSerializationSchema
 * description JsonNode 序列化器
 * version 1.0
 */
public class JsonNodeSerializationSchema implements SerializationSchema<JsonNode> {
    @Override
    public byte[] serialize(JsonNode jsonNode) {
        try {
            return jsonNode.toString().getBytes();  // 将 JsonNode 转换为字节数组
        } catch (Exception e) {
            throw new RuntimeException("Error serializing JsonNode", e);
        }
    }
}
