package com.oneself.demo;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author liuhuan
 * date 2025/3/11
 * packageName com.oneself.demo
 * className DoubleStreamInquire
 * description 双 Kafka 双流数据聚合查询 flink sql 实现
 * version 1.0
 */
public class DoubleStreamInquire2 {
    public static void main(String[] args) throws Exception {
        // 创建 Flink 流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度为 1 并行度大小
        env.setParallelism(1);
        // 设置表环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        // Kafka 源表 1 定义
        String kafkaSourceDDL1 = "CREATE TABLE kafka_source1 ("
                + " ip STRING,"
                + " port STRING,"
                + " sendReqTime BIGINT,"
                + " ts AS TO_TIMESTAMP_LTZ(sendReqTime, 3)," // 将 BIGINT 转换为 TIMESTAMP
                + " WATERMARK FOR ts AS ts - INTERVAL '5' SECOND" // 基于 TIMESTAMP 计算 WATERMARK
                + ") WITH ("
                + " 'connector' = 'kafka',"
                + " 'topic' = 'liuhuan-test-topic1',"
                + " 'properties.bootstrap.servers' = '192.168.199.105:9092',"
                + "  'properties.group.id' = 'test1',"
                + " 'format' = 'json',"
                + " 'scan.startup.mode' = 'latest-offset'"
                + ")";
        tableEnv.executeSql(kafkaSourceDDL1);
        // Kafka 源表 2 定义
        String kafkaSourceDDL2 = "CREATE TABLE kafka_source2 ("
                + " ip STRING,"
                + " port STRING,"
                + " sendReqTime BIGINT,"
                + " ts AS TO_TIMESTAMP_LTZ(sendReqTime, 3)," // 将 BIGINT 转换为 TIMESTAMP
                + " WATERMARK FOR ts AS ts - INTERVAL '5' SECOND" // 基于 TIMESTAMP 计算 WATERMARK
                + ") WITH ("
                + " 'connector' = 'kafka',"
                + " 'topic' = 'liuhuan-test-topic2',"
                + " 'properties.bootstrap.servers' = '192.168.199.105:9092',"
                + "  'properties.group.id' = 'test2',"
                + " 'format' = 'json',"
                + " 'scan.startup.mode' = 'latest-offset'"
                + ")";
        tableEnv.executeSql(kafkaSourceDDL2);


        // SQL 查询 查询 1 分钟内，在 kafka_source1 出现了，但是在 kafka_source2 中没有出现的 ip 和 port
        String query = "SELECT " +
                "k1.ip AS ip, " +
                "k1.port AS port, " +
                "TUMBLE_START(k1.ts, INTERVAL '1' MINUTE) AS windowStart, " +
                "TUMBLE_END(k1.ts, INTERVAL '1' MINUTE) AS windowEnd " +
                "FROM kafka_source1 k1 " +
                "LEFT JOIN kafka_source2 k2 " +
                "ON k1.ip = k2.ip AND k1.port = k2.port " +
                "WHERE k2.ip IS NULL " + // 只返回 kafka_source1 中存在，但 kafka_source2 中没有匹配的记录
                "GROUP BY " +
                "TUMBLE(k1.ts, INTERVAL '1' MINUTE), " + // 使用 GROUP BY 进行时间窗口聚合
                "k1.ip, k1.port;";

        TableResult tableResult = tableEnv.executeSql(query);
        tableResult.print();
    }
}