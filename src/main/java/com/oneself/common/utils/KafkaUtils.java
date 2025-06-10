package com.oneself.common.utils;

import com.oneself.common.properties.enums.KafkaPropertiesEnum;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @author liuhuan
 * date 2025/5/29
 * packageName com.oneself.common.utils
 * className KafkaUtils
 * description Kafka 工具类
 * version 1.0
 */
public class KafkaUtils {

    /**
     * 创建 KafkaSource 实例，用于从 Kafka 消费数据。
     * <p>
     * 必须在参数中配置以下字段：
     * <ul>
     *     <li>{@code source.kafka.servers} - Kafka 集群的 bootstrap servers 地址</li>
     *     <li>{@code source.kafka.topics} - 要订阅的 Kafka topic，多个 topic 用逗号分隔</li>
     *     <li>{@code source.kafka.group.id} - Kafka 消费组 ID</li>
     * </ul>
     *
     * @param parameterTool Flink 参数工具，用于获取配置参数
     * @param schema        反序列化 schema，用于将 Kafka 消息反序列化为 T 类型对象
     * @param <T>           反序列化后数据的类型
     * @return 构建好的 KafkaSource 实例
     */
    public static <T> KafkaSource<T> getKafkaSource(ParameterTool parameterTool, DeserializationSchema<T> schema) {
        // 获取数据源 Kafka 信息
        String sourceKafkaServices = parameterTool.get(KafkaPropertiesEnum.SOURCE_KAFKA_SERVERS.getKey());
        String sourceKafkaTopics = parameterTool.get(KafkaPropertiesEnum.SOURCE_KAFKA_TOPICS.getKey());
        String sourceKafkaGroupId = parameterTool.get(KafkaPropertiesEnum.SOURCE_KAFKA_GROUP_ID.getKey());

        return KafkaSource.<T>builder()
                .setBootstrapServers(sourceKafkaServices)
                .setTopics(sourceKafkaTopics)
                .setGroupId(sourceKafkaGroupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(schema)
                .build();
    }

    /**
     * 创建 KafkaSink 实例，用于将数据写入 Kafka。
     * <p>
     * 必须在参数中配置以下字段：
     * <ul>
     *     <li>{@code sink.kafka.servers} - Kafka 集群的 bootstrap servers 地址</li>
     *     <li>{@code sink.kafka.topic} - 目标 Kafka topic</li>
     * </ul>
     *
     * @param parameterTool       Flink 参数工具
     * @param serializationSchema Kafka 序列化 schema
     * @param <T>                 要写入的数据类型
     * @return KafkaSink 实例
     */
    public static <T> KafkaSink<T> getKafkaSink(ParameterTool parameterTool, SerializationSchema<T> serializationSchema) {
        String sinkKafkaServers = parameterTool.get(KafkaPropertiesEnum.SINK_KAFKA_SERVERS.getKey());
        String sinkKafkaTopic = parameterTool.get(KafkaPropertiesEnum.SINK_KAFKA_TOPICS.getKey());

        Properties producerProps = new Properties();
        producerProps.setProperty(ProducerConfig.ACKS_CONFIG, "all"); // 可选配置，根据需要添加

        return KafkaSink.<T>builder()
                .setBootstrapServers(sinkKafkaServers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<T>builder()
                                .setTopic(sinkKafkaTopic)
                                .setValueSerializationSchema(serializationSchema)
                                .build()
                )
                .setKafkaProducerConfig(producerProps)
                .build();
    }
}
