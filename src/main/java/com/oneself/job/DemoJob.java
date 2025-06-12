package com.oneself.job;

import com.fasterxml.jackson.databind.JsonNode;
import com.oneself.common.deserialization.JsonNodeDeserializationSchema;
import com.oneself.common.serialization.JsonNodeSerializationSchema;
import com.oneself.common.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author liuhuan
 * date 2025/5/29
 * packageName com.oneself.job
 * className DemoJob
 * description 任务样例
 * version 1.0
 */
public class DemoJob {
    private static final Logger log = LoggerFactory.getLogger(DemoJob.class);

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 设置并行度

        // 2. 初始化参数工具
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(parameterTool); // 设置全局参数

        // 3. 使用 KafkaUtils 获取 KafkaSource
        KafkaSource<JsonNode> kafkaSource = KafkaUtils.getKafkaSource(parameterTool, new JsonNodeDeserializationSchema());

        // 4. 从 Kafka 数据源创建流
        DataStreamSource<JsonNode> streamSource = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source",
                TypeInformation.of(JsonNode.class)
        );

        // 5. 处理数据逻辑
        // ……

        // 6. 输出结果到 Kafka Sink
        KafkaSink<JsonNode> kafkaSink = KafkaUtils.getKafkaSink(parameterTool, new JsonNodeSerializationSchema());
        streamSource.sinkTo(kafkaSink);

        // 7. 执行任务
        env.execute("Demo Job");
    }


}
