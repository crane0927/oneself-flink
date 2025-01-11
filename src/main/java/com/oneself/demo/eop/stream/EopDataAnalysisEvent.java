package com.oneself.demo.eop.stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.oneself.demo.eop.stream.function.EopProcessWindowFunction;
import com.oneself.deserialization.JsonNodeDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * @author liuhuan
 * date 2025/1/9
 * packageName com.oneself.demo.eop.stream
 * className EopDataAnalysisEvent
 * description EOP 数据解析（事件窗口） 统计 1 分钟窗口内，appId 和 apiId 相同的记录中 resultCode = 0 的占比
 * version 1.0
 */
public class EopDataAnalysisEvent {
    private static final Logger log = LoggerFactory.getLogger(EopDataAnalysisEvent.class);

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 配置 Kafka 数据源
        KafkaSource<JsonNode> kafkaSource = KafkaSource.<JsonNode>builder()
                .setBootstrapServers("192.168.199.105:9092") // Kafka 的 broker 地址
                .setTopics("liuhuan-test-topic4")             // Kafka 主题
                .setGroupId("test")                           // 消费者组 ID
                .setStartingOffsets(OffsetsInitializer.latest()) // 从最新偏移量开始消费
                .setValueOnlyDeserializer(new JsonNodeDeserializationSchema()) // 自定义反序列化器
                .build();

        // 3. 从 Kafka 数据源创建流
        DataStreamSource<JsonNode> streamSource = env.fromSource(
                kafkaSource, WatermarkStrategy.noWatermarks(),
                "Kafka Source",
                TypeInformation.of(JsonNode.class)); // 显式指定类型信息

        // 4. 分配时间戳和水位线，支持乱序数据
        WatermarkStrategy<JsonNode> watermarkStrategy = WatermarkStrategy
                .<JsonNode>forBoundedOutOfOrderness(Duration.ofSeconds(5)) // 允许 5 秒的乱序数据
                .withTimestampAssigner((element, recordTimestamp) -> {
                    if (element.has("sendReqTime")) {
                        log.info("数据: {}, 记录时间: {}", element, recordTimestamp);
//                        return element.get("sendReqTime").asLong();
                        return recordTimestamp;
                    } else {
                        log.warn("消息中缺少 sendReqTime 字段: {}", element);
                        return recordTimestamp; // 或者使用当前系统时间
                    }
                });
        SingleOutputStreamOperator<JsonNode> events = streamSource.assignTimestampsAndWatermarks(watermarkStrategy);

        // 5. 按 appId 和 apiId 分组，并设置 1 分钟滚动窗口
        SingleOutputStreamOperator<JsonNode> process = events
                .keyBy(new KeySelector<JsonNode, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(JsonNode value) {
                        String appId = value.has("appId") ? value.get("appId").asText() : "unknown-app";
                        String apiId = value.has("apiId") ? value.get("apiId").asText() : "unknown-api";
                        return Tuple2.of(appId, apiId);
                    }
                })
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10))) // 事件时间窗口 1 分钟
//                .allowedLateness(Time.seconds(5))  // 允许迟到 5 秒的数据
                .process(new EopProcessWindowFunction());
        process.print("结果数据");

        // 6. 启动任务
        env.execute("EOP 错误率和告警监控");
    }
}

