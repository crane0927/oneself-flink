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
 * description MySQL、Kafka 双流数据聚合查询
 * version 1.0
 */
public class DoubleStreamInquire {
    public static void main(String[] args) throws Exception {
        // 创建 Flink 流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度为 1 并行度大小
        env.setParallelism(1);
        // 设置表环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // Kafka 源表定义
        String kafkaSourceDDL = "CREATE TABLE kafka_source_table ("
                + " ip STRING,"
                + " port STRING,"
                + " sendReqTime BIGINT,"
                + " ts AS TO_TIMESTAMP_LTZ(sendReqTime, 3)," // 将 BIGINT 转换为 TIMESTAMP
                + " WATERMARK FOR ts AS ts - INTERVAL '5' SECOND" // 基于 TIMESTAMP 计算 WATERMARK
                + ") WITH ("
                + " 'connector' = 'kafka',"
                + " 'topic' = 'liuhuan-test-topic2',"
                + " 'properties.bootstrap.servers' = '192.168.199.105:9092',"
                + "  'properties.group.id' = 'test',"
                + " 'format' = 'json',"
                + " 'scan.startup.mode' = 'latest-offset'"
                + ")";
        tableEnv.executeSql(kafkaSourceDDL);

        // MySQL 表定义
        String mysqlDDL = "CREATE TABLE mysql_table ("
                + " id INT NOT NULL,"
                + " ip VARCHAR(255) NOT NULL,"
                + " port VARCHAR(255) NOT NULL,"
                + " PRIMARY KEY (id) NOT ENFORCED"
                + ") WITH ("
                + " 'connector' = 'jdbc',"
                + " 'url' = 'jdbc:mysql://localhost:3306/test',"
                + " 'table-name' = 'double_stream_mysql',"
                + " 'username' = 'liuhuan',"
                + " 'password' = 'Z6eQaNaitK5vSX',"
                + " 'driver' = 'com.mysql.cj.jdbc.Driver'"
                + ")";
//        tableEnv.executeSql(mysqlDDL);

        // MySQL CDC 源表定义，流式处理
        String mysqlCDCSourceDDL = "CREATE TABLE mysql_table ("
                + " id INT NOT NULL,"
                + " ip VARCHAR(255) NOT NULL,"
                + " port VARCHAR(255) NOT NULL,"
                + " PRIMARY KEY (id) NOT ENFORCED"
                + ") WITH ("
                + " 'connector' = 'mysql-cdc',"
                + " 'hostname' = 'localhost',"
                + " 'port' = '3306',"
                + " 'username' = 'liuhuan',"
                + " 'password' = 'Z6eQaNaitK5vSX',"
                + " 'database-name' = 'test',"
                + " 'table-name' = 'double_stream_mysql',"
                + " 'server-id' = '12345',"
                + " 'debezium.snapshot.mode' = 'initial' "
                + ")";
        tableEnv.executeSql(mysqlCDCSourceDDL);

        // SQL 查询
        String query = "SELECT " +
                "k.ip as ip, " +
                "k.port as port, " +
                "TUMBLE_START(k.ts, INTERVAL '1' MINUTE) AS windowStart, " + // 窗口开始时间
                "TUMBLE_END(k.ts, INTERVAL '1' MINUTE) AS windowEnd, " + // 窗口结束时间
                "COUNT(*) AS num " +
                "FROM kafka_source_table k " +
                "JOIN mysql_table m " +
                "ON k.ip = m.ip AND k.port = m.port " +
                "GROUP BY " +
                "TUMBLE(k.ts, INTERVAL '1' MINUTE), " +
                "k.ip, " +
                "k.port;";

        query = "select * from mysql_table";


        TableResult tableResult = tableEnv.executeSql(query);
        tableResult.print();

    }
}