package com.oneself.demo.transformation;

import com.oneself.deserialization.UserDeserializationSchema;
import com.oneself.model.pojo.User;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author liuhuan
 * date 2025/1/8
 * packageName com.oneself.demo.transformation
 * className Trans
 * description 转换算子
 * version 1.0
 */
public class Trans {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 从 kafka 读取数据
        KafkaSource<User> kafkaSource = KafkaSource.<User>builder()
                .setBootstrapServers("192.168.199.105:9092")
                .setTopics("liuhuan-test-topic4")
                .setGroupId("test")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new UserDeserializationSchema())
                .build();
        // 3. 将数据源添加到执行环境
        DataStreamSource<User> streamSource = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // 4. 转换算子 filter 获取 id 为 1 的 user
        SingleOutputStreamOperator<User> filter = streamSource.filter(new FilterFunction<User>() {
            @Override
            public boolean filter(User user) throws Exception {
                return 1 == user.getId();
            }
        });
        filter.print();

        // 5. 转换算子 map 获取 user 的 age
//        filter.map(new MapFunction<User, Integer>() {
//            @Override
//            public Integer map(User value) throws Exception {
//                return value.getAge();
//            }
//        }).print();
        filter.map((MapFunction<User, Integer>) User::getAge).print();

        // 6. 启动程序
        env.execute();
    }
}
