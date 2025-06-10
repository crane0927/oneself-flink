package com.oneself.demo.source;

import com.oneself.common.properties.enums.KafkaPropertiesEnum;
import com.oneself.common.utils.OneselfPropertiesUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author liuhuan
 * date 2025/1/8
 * packageName com.oneself.demo.source
 * className SourceKafka2
 * description 从 Kafka 中读取数据 Kafka 配置从配置文件中获取
 * version 1.0
 */
public class SourceKafka2 {
    public static void main(String[] args) throws Exception {
        // 0. 参数设置
        ParameterTool parameterTool = OneselfPropertiesUtils.initParameter(args);
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 配置数据源 kafka
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(parameterTool.get(KafkaPropertiesEnum.SOURCE_KAFKA_SERVERS.getKey())) // 指定 Kafka 集群地址，多个地址用逗号分隔
                .setTopics(parameterTool.get(KafkaPropertiesEnum.SOURCE_KAFKA_TOPICS.getKey())) // 指定要读取的 topic，多个地址用逗号分隔
                .setGroupId(parameterTool.get(KafkaPropertiesEnum.SOURCE_KAFKA_GROUP_ID.getKey())) // 指定消费组 ID
                .setStartingOffsets(OffsetsInitializer.latest()) // 从最新的偏移量开始读取数据
                .setValueOnlyDeserializer(new SimpleStringSchema()) //  指定反序列化器，只读取 value
                .build();

        // 3. 将数据源添加到执行环境
        DataStreamSource<String> streamSource = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");
        // 4. 打印数据
        streamSource.print("Kafka");

        // 5. 启动程序
        env.execute();
    }
}
