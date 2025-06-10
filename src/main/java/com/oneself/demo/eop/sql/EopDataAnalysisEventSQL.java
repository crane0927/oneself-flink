package com.oneself.demo.eop.sql;


import com.oneself.demo.eop.common.CommonSQL;
import com.oneself.common.properties.enums.KafkaPropertiesEnum;
import com.oneself.common.properties.enums.OneselfPropertiesEnum;
import com.oneself.common.utils.OneselfPropertiesUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author liuhuan
 * date 2025/1/10
 * packageName com.oneself.demo.eop.sql
 * className EopDataAnalysisEventSQL
 * description Flink SQL 实现 EOP 错误率和告警监控（事件时间）
 * version 1.0
 */
public class EopDataAnalysisEventSQL {
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = OneselfPropertiesUtils.initParameter(args);
        // 1. 创建流式 Table 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 并行度设置
        String parallelismEventTime = parameterTool.get(OneselfPropertiesEnum.PARALLELISM_EVENT_TIME.getKey());
        if (ObjectUtils.isNotEmpty(parallelismEventTime)) {
            env.setParallelism(Integer.parseInt(parallelismEventTime));
        }


        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 获取数据源 Kafka 信息
        String sourceKafkaServices = parameterTool.get(KafkaPropertiesEnum.SOURCE_KAFKA_SERVERS.getKey());
        String sourceKafkaTopics = parameterTool.get(KafkaPropertiesEnum.SOURCE_KAFKA_TOPICS.getKey());
        String sourceKafkaGroupId = parameterTool.get(KafkaPropertiesEnum.SOURCE_KAFKA_GROUP_ID.getKey());

        // 获取目标 Kafka 信息
        String sinkKafkaServices = parameterTool.get(KafkaPropertiesEnum.SINK_KAFKA_SERVERS.getKey());
        String sinkKafkaTopics = parameterTool.get(KafkaPropertiesEnum.SINK_KAFKA_TOPICS.getKey());

        // 2. 注册 Kafka 数据源表
        String kafkaSourceDDL = "CREATE TABLE kafka_source (\n" +
                CommonSQL.COMMON_CREATE_ROWS +
                "  sendReqTime BIGINT, \n" + // 定义为毫秒级时间戳
                "  `ts` as TO_TIMESTAMP(FROM_UNIXTIME(sendReqTime/1000, 'yyyy-MM-dd HH:mm:ss')), \n" +
                "  resultCode INT, \n" +
                "  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND\n" + // 定义水位线
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '" + sourceKafkaTopics + "',\n" +
                "  'properties.bootstrap.servers' = '" + sourceKafkaServices + "',\n" +
                "  'properties.group.id' = '" + sourceKafkaGroupId + "',\n" +
                "  'format' = 'json',\n" +
                "  'scan.startup.mode' = 'latest-offset'\n" +
                ");";
        tableEnv.executeSql(kafkaSourceDDL);

        // 3. 定义基于事件时间的 SQL 查询
        String query = "SELECT\n" +
                CommonSQL.COMMON_GROUP_ROWS +
                "  COUNT(*) AS total_count,\n" +  // 总条数
                "  SUM(CASE WHEN resultCode <> 0 THEN 1 ELSE 0 END) AS error_count,\n" + // 错误条数
                "  ROUND(CAST(SUM(CASE WHEN resultCode = 0 THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) * 100, 2) AS success_rate, \n" + // 成功比例
                "  TUMBLE_START(ts, INTERVAL '1' MINUTE) AS window_start,\n" + // 窗口开始时间
                "  TUMBLE_END(ts, INTERVAL '1' MINUTE) AS window_end\n" + // 窗口结束时间
                "FROM kafka_source\n" +
                "GROUP BY\n" +
                CommonSQL.COMMON_GROUP_ROWS +
                "  TUMBLE(ts, INTERVAL '1' MINUTE);"; // 1 分钟滚动窗口

        // 4. 执行查询并将结果写入目标 Kafka
        String sinkKafkaDDL = "CREATE TABLE sink_kafka (\n" +
                CommonSQL.COMMON_CREATE_ROWS +
                "  totalCount BIGINT, \n" + // 总条数
                "  errorCount BIGINT, \n" + // 错误条数
                "  successRate DOUBLE, \n" + // 成功比例
                "  windowStart TIMESTAMP(3), \n" + // 窗口开始时间
                "  windowEnd TIMESTAMP(3)\n" + // 窗口结束时间
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '" + sinkKafkaTopics + "',\n" +
                "  'properties.bootstrap.servers' = '" + sinkKafkaServices + "',\n" +
                "  'format' = 'json'\n" +
                ");";

        tableEnv.executeSql(sinkKafkaDDL);
        Table resultTable = tableEnv.sqlQuery(query);
        // 5. 将查询结果写入目标 Kafka
        resultTable.executeInsert("sink_kafka");
        // 5. 执行查询并输出到控制台
//        tableEnv.toChangelogStream(resultTable).print();

        // 6. 启动任务 当使用 table sql 没有流算子的存在时 不需要加
//        env.execute("EOP 错误率和告警监控");
    }
}
