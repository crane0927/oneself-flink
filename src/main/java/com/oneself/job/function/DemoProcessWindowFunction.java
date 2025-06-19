package com.oneself.job.function;

import com.fasterxml.jackson.databind.JsonNode;
import com.oneself.common.utils.JacksonUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;

/**
 * @author liuhuan
 * date 2025/1/9
 * packageName com.oneself.example.eop.stream.function
 * className DemoProcessWindowFunction
 * description
 * version 1.0
 */
public class DemoProcessWindowFunction extends ProcessWindowFunction<JsonNode, JsonNode, Tuple2<String, Integer>, TimeWindow> {

    @Override
    public void process(Tuple2<String, Integer> key, ProcessWindowFunction<JsonNode, JsonNode, Tuple2<String, Integer>, TimeWindow>.Context context, Iterable<JsonNode> elements, Collector<JsonNode> out) throws Exception {
        // 数据处理逻辑
        HashMap<String, Object> map = new HashMap<>();
        // ……
        out.collect(JacksonUtils.toJsonNode(map));
    }
}
