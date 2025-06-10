package com.oneself.demo;

import com.oneself.common.ops.redis.RedisOps;
import com.oneself.common.ops.redis.RedisOpsFactory;
import com.oneself.common.properties.RedisProperties;
import com.oneself.common.utils.OneselfPropertiesUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

/**
 * @author liuhuan
 * date 2025/1/10
 * packageName com.oneself.demo
 * className RedisRuleAutoProcessing
 * description 从 Redis 中获取单个任务下的多个规则，同时进行处理
 * version 1.0
 */
public class RedisRuleAutoProcessing {

    private static final Logger log = LoggerFactory.getLogger(RedisRuleAutoProcessing.class);

    /**
     * 定义 Kafka 表的 DDL 模板
     */
    private static final String KAFKA_DDL = "CREATE TABLE %s (%s \n" +
            ") WITH (\n" +
            "  'connector' = 'kafka',\n" + // 使用 Kafka 作为数据源
            "  'topic' = '%s',\n" + // Kafka topic 名称
            "  'properties.bootstrap.servers' = '%s',\n" + // Kafka 地址
            "  'properties.group.id' = '%s',\n" + // Kafka 消费组 ID
            "  'format' = 'json',\n" + // 数据格式为 JSON
            "  'scan.startup.mode' = 'latest-offset'\n" + // 从最新的偏移量开始读取
            ");";

    public static void main(String[] args) {
        ParameterTool parameterTool = OneselfPropertiesUtils.initParameter(args);
        // 1. 创建 Flink 流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 创建 Flink 表环境，使用流处理
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 模拟任务 ID 从启动参数获取
        String taskId = "task_id";
        try {
            RedisProperties redisProperties = new RedisProperties(parameterTool);
            RedisOps redisOps = RedisOpsFactory.create(redisProperties);
            // 3. 从 Redis 获取 Kafka 数据源的字段信息
            String sourceFields = redisOps.get(taskId + "_fields");
            if (sourceFields == null || sourceFields.isEmpty()) {
                throw new IllegalStateException("Redis 获取数据源建表字段为空");
            }

            // 4. 创建 Kafka 数据源表的 DDL
            String kafkaSourceDDL = String.format(KAFKA_DDL, "kafka_source_" + taskId, sourceFields, "liuhuan-test-topic4", "192.168.199.105:9092", "test");
            log.info("Kafka Source DDL: {}", kafkaSourceDDL);
            // 执行 Kafka 源表的 DDL，注册到 Flink
            tableEnv.executeSql(kafkaSourceDDL);

            // 5. 获取 Redis 中的任务 ID 规则集合

            Set<String> taskIdRules = redisOps.sMembers(taskId + "_rules");
            if (taskIdRules != null) {
                // 6. 遍历任务 ID 规则
                for (String taskIdRule : taskIdRules) {
                    // 获取每个任务规则的配置信息
                    Map<String, String> map = redisOps.hGetAll(taskIdRule);

                    if (map != null) {
                        // 7. 从 Redis 中读取要创建的字段和 SQL 查询语句
                        String sinkFields = map.get("fields"); // 获取目标 Kafka 表字段
                        String query = map.get("query"); // 获取 SQL 查询语句
                        query = String.format(query, "kafka_source_" + taskId, taskIdRule);
                        log.info("SQL Query: {}", query);

                        // 8. 创建 Kafka 接收表的 DDL
                        String kafkaSinkDDL = String.format(KAFKA_DDL, "kafka_sink_" + taskIdRule, sinkFields, "liuhuan-test-topic5", "192.168.199.105:9092", "test");
                        log.info("Kafka Sink DDL: {}", kafkaSinkDDL);
                        tableEnv.executeSql(kafkaSinkDDL);

                        // 9. 执行 SQL 查询，将数据插入到 Kafka 接收表
                        Table table = tableEnv.sqlQuery(query);
                        table.executeInsert("kafka_sink_" + taskIdRule);
                    }
                }
            }

        } catch (Exception e) {
            log.error("执行 Flink 作业时发生错误", e);
        }
    }
}