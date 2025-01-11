package com.oneself.demo.doris;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.JsonDebeziumSchemaSerializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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
 * packageName com.oneself.demo.doris
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
                        .tableList("test.t1") // set captured table
                        .username("root")
                        .password("123456")
                        .debeziumProperties(getProperties())
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
                        .setTableIdentifier("test.t1")
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
                .sinkTo(builder.build());

        env.execute("Print MySQL Snapshot + Binlog");
    }

    private static Properties getProperties() {
        Properties properties = new Properties();
        properties.setProperty("converters", "date");
        properties.setProperty(
                "date.type", "org.apache.doris.flink.utils.DateToStringConverter");
        properties.setProperty("date.format.date", "yyyy-MM-dd");
        properties.setProperty("date.format.datetime", "yyyy-MM-dd HH:mm:ss");
        properties.setProperty("date.format.timestamp", "yyyy-MM-dd HH:mm:ss");
        properties.setProperty("date.format.timestamp.zone", "UTC");
        return properties;
    }
}
