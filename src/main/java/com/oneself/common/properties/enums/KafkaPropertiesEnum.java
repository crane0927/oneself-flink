package com.oneself.common.properties.enums;

/**
 * @author liuhuan
 * date 2025/1/11
 * packageName com.oneself.common.properties.enums
 * enumName KafkaPropertiesEnum
 * description Kafka 配置信息枚举 映射 onesel-flink.properties 中的 Kafka 相关配置信息
 * version 1.0
 */
public enum KafkaPropertiesEnum {
    SOURCE_KAFKA_SERVERS("source.kafka.services", "数据源 kafka 服务地址"),
    SOURCE_KAFKA_TOPICS("source.kafka.topics", "数据源 kafka topic"),
    SOURCE_KAFKA_GROUP_ID("source.kafka.group.id", "数据源 kafka group id"),

    SINK_KAFKA_SERVERS("sink.kafka.services", "目标 kafka 服务地址"),
    SINK_KAFKA_TOPICS("sink.kafka.topics", "目标 kafka topic"),
    SINK_KAFKA_GROUP_ID("sink.kafka.group.id", "目标 kafka group id");

    private final String key;
    private final String description;

    KafkaPropertiesEnum(String key, String description) {
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
