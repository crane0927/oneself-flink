package com.oneself.common.deserialization;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.oneself.common.utils.JacksonUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author liuhuan
 * date 2025/1/8
 * packageName com.oneself.common.deserialization
 * className JsonNodeDeserializationSchema
 * description JsonNode 反序列化器
 * version 1.0
 */
public class JsonNodeDeserializationSchema implements DeserializationSchema<JsonNode> {

    private static final Logger log = LoggerFactory.getLogger(JsonNodeDeserializationSchema.class);

    private static final ObjectMapper OBJECT_MAPPER = JacksonUtils.getObjectMapper();


    @Override
    public JsonNode deserialize(byte[] bytes) throws IOException {
        return OBJECT_MAPPER.readTree(bytes);
    }

    @Override
    public boolean isEndOfStream(JsonNode jsonNode) {
        return false;
    }

    @Override
    public TypeInformation<JsonNode> getProducedType() {
        return TypeInformation.of(JsonNode.class);
    }
}
