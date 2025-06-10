package com.oneself.demo;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.oneself.common.serialization.JsonNodeSerializationSchema;
import com.oneself.common.deserialization.JsonNodeDeserializationSchema;
import com.oneself.common.utils.JacksonUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * @author liuhuan
 * date 2025/3/12
 * packageName com.oneself.demo
 * className DoubleStreamInquire
 * description 双 Kafka 双流数据聚合查询 data stream api 实现
 * version 1.0
 */
public class DoubleStreamInquire3 {
    public static void main(String[] args) throws Exception {
        // 创建 Flink 流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Kafka 数据源 1
        KafkaSource<JsonNode> kafkaSource1 = KafkaSource.<JsonNode>builder()
                .setBootstrapServers("192.168.199.105:9092")
                .setTopics("liuhuan-test-topic1")
                .setGroupId("test1")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new JsonNodeDeserializationSchema())
                .build();

        // Kafka 数据源 2
        KafkaSource<JsonNode> kafkaSource2 = KafkaSource.<JsonNode>builder()
                .setBootstrapServers("192.168.199.105:9092")
                .setTopics("liuhuan-test-topic2")
                .setGroupId("test2")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new JsonNodeDeserializationSchema())
                .build();

        // 从 Kafka 创建流
        DataStreamSource<JsonNode> streamSource1 = env.fromSource(
                kafkaSource1, WatermarkStrategy.noWatermarks(), "Kafka Source1");

        DataStreamSource<JsonNode> streamSource2 = env.fromSource(
                kafkaSource2, WatermarkStrategy.noWatermarks(), "Kafka Source2");

        // 分配时间戳和水位线，支持乱序数据
        WatermarkStrategy<JsonNode> watermarkStrategy = WatermarkStrategy
                .<JsonNode>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((element, recordTimestamp) ->
                        element.has("sendReqTime") ? element.get("sendReqTime").asLong() : recordTimestamp);

        SingleOutputStreamOperator<JsonNode> events1 = streamSource1.assignTimestampsAndWatermarks(watermarkStrategy);
        SingleOutputStreamOperator<JsonNode> events2 = streamSource2.assignTimestampsAndWatermarks(watermarkStrategy);


        // 按 Tuple2<ip, port>  进行 KeyBy 不能用 lambada 表达式，会报错
        KeyedStream<JsonNode, Tuple2<String, String>> keyedStream1 = events1.keyBy(
                new KeySelector<JsonNode, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(JsonNode jsonNode) {
                        return Tuple2.of(jsonNode.get("ip").asText(), jsonNode.get("port").asText());
                    }
                }
        );

        KeyedStream<JsonNode, Tuple2<String, String>> keyedStream2 = events2.keyBy(
                new KeySelector<JsonNode, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(JsonNode jsonNode) {
                        return Tuple2.of(jsonNode.get("ip").asText(), jsonNode.get("port").asText());
                    }
                }
        );
        // 处理双流比对
        SingleOutputStreamOperator<JsonNode> unmatchedRecords = keyedStream1
                .connect(keyedStream2)
                .process(new MatchDetectProcessFunction());


        // 创建 Kafka Sink，将 unmatchedRecords 推送到目标 Kafka
        KafkaSink<JsonNode> kafkaSink = KafkaSink.<JsonNode>builder()
                .setBootstrapServers("192.168.199.105:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("liuhuan-test-topic3")
                        .setValueSerializationSchema(new JsonNodeSerializationSchema())
                        .build())
                .build();

        // 将 unmatchedRecords 发送到 Kafka
        unmatchedRecords.sinkTo(kafkaSink);

        env.execute();
    }


    /**
     * MatchDetectProcessFunction 负责处理两个 Kafka 数据流 (source1 和 source2)，
     * 并检测 source1 中的 (ip, port) 是否在 source2 中出现过。如果 1 分钟后 source2 仍未匹配到相应的 (ip, port)，
     * 则输出未匹配的记录。
     *
     * <p>主要逻辑：
     * <ul>
     *     <li>从 Kafka Source1 读取数据，提取 (ip, port) 及 sendReqTime，并存入状态。</li>
     *     <li>注册一个 1 分钟后的定时器，以检测是否匹配到相应数据。</li>
     *     <li>从 Kafka Source2 读取数据，标记 (ip, port) 是否出现过。</li>
     *     <li>当定时器触发时，检查 source2 是否匹配过该 (ip, port)，
     *         如果没有匹配到，则输出该未匹配的记录。</li>
     * </ul>
     *
     * <p>该函数继承 {@link CoProcessFunction}，用于处理两个数据流的联合检测。
     */
    public static class MatchDetectProcessFunction extends CoProcessFunction<JsonNode, JsonNode, JsonNode> {

        /**
         * 存储 Kafka Source1 中 (ip, port) 的 sendReqTime 作为时间戳。
         */
        private ValueState<Long> source1TimestampState;

        /**
         * 记录 Kafka Source2 是否匹配过该 (ip, port)。
         */
        private ValueState<Boolean> source2SeenState;

        /**
         * 维护定时器时间戳与 (ip, port) 的映射关系，
         * 便于在定时器触发时查找相应的 ip 和 port。
         */
        private MapState<Long, Tuple2<String, String>> timerKeyMapState;

        /**
         * Jackson 的 ObjectMapper，用于构造 JsonNode。
         */
        private transient ObjectMapper objectMapper;

        /**
         * 初始化状态变量和 ObjectMapper。
         *
         * @param parameters Flink 任务的运行参数
         * @throws Exception 如果初始化状态变量失败
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            // 初始化 `source1TimestampState`，用于存储 `kafka_source1` 事件的时间戳
            ValueStateDescriptor<Long> source1TimestampDescriptor = new ValueStateDescriptor<>(
                    "source1TimestampState",
                    TypeInformation.of(Long.class)
            );
            source1TimestampState = getRuntimeContext().getState(source1TimestampDescriptor);

            // 初始化 `source2SeenState`，用于标记 `kafka_source2` 是否出现过相同 (ip, port)
            ValueStateDescriptor<Boolean> source2SeenDescriptor = new ValueStateDescriptor<>(
                    "source2SeenState",
                    TypeInformation.of(Boolean.class)
            );
            source2SeenState = getRuntimeContext().getState(source2SeenDescriptor);

            // 初始化 `timerKeyMapState`，用于存储定时器时间戳和 (ip, port) 的映射关系
            MapStateDescriptor<Long, Tuple2<String, String>> timerKeyMapDescriptor = new MapStateDescriptor<>(
                    "timerKeyMapState",
                    TypeInformation.of(Long.class),
                    TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
                    })
            );
            timerKeyMapState = getRuntimeContext().getMapState(timerKeyMapDescriptor);

            // 初始化 ObjectMapper，用于 JSON 解析和创建 JSON 结果
            objectMapper = JacksonUtils.getObjectMapper();
        }

        /**
         * 处理 Kafka Source1 数据，提取 (ip, port) 及 sendReqTime，并注册定时器。
         *
         * @param jsonNode  输入的 JSON 数据
         * @param context   Flink 处理上下文
         * @param collector 数据收集器
         * @throws Exception 如果状态更新失败
         */
        @Override
        public void processElement1(JsonNode jsonNode, Context context, Collector<JsonNode> collector) throws Exception {
            String ip = jsonNode.get("ip").asText();
            String port = jsonNode.get("port").asText();
            long sendReqTime = jsonNode.get("sendReqTime").asLong();

            // 存储当前 (ip, port) 记录的时间戳
            source1TimestampState.update(sendReqTime);

            // 设置定时器，1 分钟后触发
            long timerTimestamp = sendReqTime + 60000;
            context.timerService().registerEventTimeTimer(timerTimestamp);

            // 存储定时器时间戳与 (ip, port) 的映射关系
            timerKeyMapState.put(timerTimestamp, Tuple2.of(ip, port));
        }

        /**
         * 处理 Kafka Source2 数据，标记该 (ip, port) 是否出现过。
         *
         * @param jsonNode  输入的 JSON 数据
         * @param context   Flink 处理上下文
         * @param collector 数据收集器
         * @throws Exception 如果状态更新失败
         */
        @Override
        public void processElement2(JsonNode jsonNode, Context context, Collector<JsonNode> collector) throws Exception {
            // 提取 ip 和 port
//            String ip = jsonNode.get("ip").asText();
//            String port = jsonNode.get("port").asText();

            // 标记该 (ip, port) 在 `kafka_source2` 中出现过
            source2SeenState.update(true);
        }

        /**
         * 当定时器触发时，检查 (ip, port) 是否在 Source2 中出现过，
         * 如果未匹配到，则输出该 (ip, port) 及其原始时间戳。
         *
         * @param timestamp 触发定时器的时间戳
         * @param ctx       定时器上下文
         * @param out       输出收集器
         * @throws Exception 如果状态访问失败
         */
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<JsonNode> out) throws Exception {
            // 从 MapState 获取 (ip, port) 信息
            Tuple2<String, String> ipPortTuple = timerKeyMapState.get(timestamp);

            if (ipPortTuple != null) {
                String ip = ipPortTuple.f0;
                String port = ipPortTuple.f1;

                // 检查 `kafka_source2` 中是否出现过该 (ip, port)
                Boolean seenInSource2 = source2SeenState.value();
                if (seenInSource2 == null || !seenInSource2) {
                    // 如果 `kafka_source2` 中 **未出现过** 该 (ip, port)，则输出记录
                    Long source1Timestamp = source1TimestampState.value();
                    if (source1Timestamp != null) {
                        // 构造输出 JSON 记录
                        ObjectNode result = objectMapper.createObjectNode();
                        result.put("ip", ip);
                        result.put("port", port);
                        result.put("sendReqTime", source1Timestamp);
//                        result.put("time",formatTimestamp(source1Timestamp));

                        // 输出该记录
                        out.collect(result);
                    }
                }

                // 清理状态，防止状态膨胀
                source1TimestampState.clear();
                source2SeenState.clear();
                timerKeyMapState.remove(timestamp);
            }
        }
    }

    /**
     * 将时间戳转换为 yyyy-MM-dd HH:mm:ss 格式的字符串
     *
     * @param timestamp 时间戳（毫秒）
     * @return 格式化后的时间字符串
     */
    private static String formatTimestamp(long timestamp) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        return Instant.ofEpochMilli(timestamp)
                .atZone(ZoneId.systemDefault())
                .format(formatter);
    }
}
