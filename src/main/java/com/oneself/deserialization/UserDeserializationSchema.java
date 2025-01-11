package com.oneself.deserialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.oneself.model.pojo.User;
import com.oneself.utils.JacksonUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import java.io.IOException;

/**
 * @author liuhuan
 * date 2025/1/8
 * packageName com.oneself.deserialization
 * className UserDeserializationSchema
 * description User pojo 类反序列化器
 * version 1.0
 */
public class UserDeserializationSchema implements DeserializationSchema<User> {

    private static final ObjectMapper OBJECT_MAPPER = JacksonUtils.getObjectMapper();

    @Override
    public void open(InitializationContext context) throws Exception {
        DeserializationSchema.super.open(context);
    }

    @Override
    public User deserialize(byte[] bytes) throws IOException {
        return OBJECT_MAPPER.readValue(bytes, User.class);
    }

    @Override
    public void deserialize(byte[] message, Collector<User> out) throws IOException {
        DeserializationSchema.super.deserialize(message, out);
    }

    @Override
    public boolean isEndOfStream(User user) {
        return false;
    }

    @Override
    public TypeInformation<User> getProducedType() {
        return TypeInformation.of(User.class);
    }


}
