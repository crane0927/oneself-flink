package com.oneself.task.demo;

import com.fasterxml.jackson.databind.JsonNode;
import com.oneself.common.deserialization.JsonNodeDeserializationSchema;
import com.oneself.common.serialization.JsonNodeSerializationSchema;
import com.oneself.common.utils.KafkaUtils;
import com.oneself.demo.eop.stream.function.EopProcessWindowFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * @author liuhuan
 * date 2025/5/29
 * packageName com.oneself.task.demo
 * className DemoTask
 * description 任务样例
 * version 1.0
 */
public class DemoTask {
    private static final Logger log = LoggerFactory.getLogger(DemoTask.class);

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

        // 5. 分配时间戳和水位线
        WatermarkStrategy<JsonNode> watermarkStrategy = WatermarkStrategy
                .<JsonNode>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((element, recordTimestamp) -> {
                    if (element.has("eventTime")) {
                        log.info("消息中存在 eventTime 字段: {}", element);
                        return element.get("eventTime").asLong();
                    } else {
                        log.warn("消息中缺少 eventTime 字段: {}", element);
                        return recordTimestamp;
                    }
                });

        SingleOutputStreamOperator<JsonNode> events = streamSource.assignTimestampsAndWatermarks(watermarkStrategy);

        // 6. 窗口计算逻辑
        SingleOutputStreamOperator<JsonNode> process = events
                .keyBy((KeySelector<JsonNode, Tuple2<String, String>>) value -> {
                    String appId = value.has("appId") ? value.get("appId").asText() : "unknown-app";
                    String apiId = value.has("apiId") ? value.get("apiId").asText() : "unknown-api";
                    return Tuple2.of(appId, apiId);
                })
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
                .process(new EopProcessWindowFunction());

        // 7. 输出结果到 Kafka Sink
        KafkaSink<JsonNode> kafkaSink = KafkaUtils.getKafkaSink(parameterTool, new JsonNodeSerializationSchema());
        process.sinkTo(kafkaSink);

        // 8. 执行任务
        env.execute("Demo");
    }


}
