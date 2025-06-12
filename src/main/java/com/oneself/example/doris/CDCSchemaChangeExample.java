package com.oneself.example.doris;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.JsonDebeziumSchemaSerializer;
import org.apache.doris.flink.tools.cdc.mysql.DateToStringConverter;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.shaded.org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;


/**
 * @author liuhuan
 * date 2025/1/3
 * packageName com.oneself.example.doris
 * className CDCSchemaChangeExample
 * description
 * version 1.0
 */
public class CDCSchemaChangeExample {
    public static void main(String[] args) throws Exception {

        Map<String, Object> customConverterConfigs = new HashMap<>();
        customConverterConfigs.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, "numeric");
        JsonDebeziumDeserializationSchema schema =
                new JsonDebeziumDeserializationSchema(false, customConverterConfigs);

        MySqlSource<String> mySqlSource =
                MySqlSource.<String>builder()
                        .hostname("127.0.0.1")
                        .port(3306)
                        .databaseList("test") // set captured database
                        .tableList("test.test_table") // set captured table
                        .username("liuhuan")
                        .password("Z6eQaNaitK5vSX")
                        .debeziumProperties(DateToStringConverter.DEFAULT_PROPS)
                        .deserializer(schema)
                        .serverTimeZone("Asia/Shanghai")
                        .includeSchemaChanges(true) // converts SourceRecord to JSON String
                        .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // enable checkpoint
        env.enableCheckpointing(10000);

        Properties props = new Properties();
        props.setProperty("format", "json");
        props.setProperty("read_json_by_line", "true");
        DorisOptions dorisOptions =
                DorisOptions.builder()
                        .setFenodes("127.0.0.1:8030")
                        .setTableIdentifier("test.test_table")
                        .setUsername("root")
                        .setPassword("123456")
                        .build();

        DorisExecutionOptions.Builder executionBuilder = DorisExecutionOptions.builder();
        executionBuilder
                .setLabelPrefix("label-doris" + UUID.randomUUID())
                .setStreamLoadProp(props)
                .setDeletable(true);

        DorisSink.Builder<String> builder = DorisSink.builder();
        builder.setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(executionBuilder.build())
                .setDorisOptions(dorisOptions)
                .setSerializer(
                        JsonDebeziumSchemaSerializer.builder()
                                .setDorisOptions(dorisOptions)
                                .setNewSchemaChange(true)
                                .build());

        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source") // .print();
                .map((MapFunction<String, String>) value -> {
                    // 打印消费到的 Kafka 消息
                    System.out.println(value);
                    return value;
                })
                .sinkTo(builder.build());

        env.execute("Print MySQL Snapshot + Binlog");
    }

}
