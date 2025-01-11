package com.oneself.demo.eop.stream.function;

import com.fasterxml.jackson.databind.JsonNode;
import com.oneself.utils.JacksonUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;

/**
 * @author liuhuan
 * date 2025/1/9
 * packageName com.oneself.demo.eop.stream.function
 * className EopProcessWindowFunction
 * description
 * version 1.0
 */
public class EopProcessWindowFunction extends ProcessWindowFunction<JsonNode, JsonNode, Tuple2<String, String>, TimeWindow> {
    @Override
    public void process(Tuple2<String, String> key, ProcessWindowFunction<JsonNode, JsonNode, Tuple2<String, String>, TimeWindow>.Context context, Iterable<JsonNode> iterable, Collector<JsonNode> collector) throws Exception {
        long totalCount = 0;
        long errorCount = 0;

        for (JsonNode event : iterable) {
            totalCount++;
            if (event.get("resultCode").asInt() != 0) {
                errorCount++;
            }
        }
        BigDecimal successRate;
        if (totalCount == 0) {
            successRate = BigDecimal.valueOf(100);
        } else {
            BigDecimal total = new BigDecimal(totalCount);
            BigDecimal error = new BigDecimal(errorCount);
            successRate = BigDecimal.ONE.subtract(error.divide(total, 10, RoundingMode.HALF_UP))
                    .multiply(new BigDecimal(100))
                    .setScale(2, RoundingMode.HALF_UP);;

        }


        long startTs = context.window().getStart();
        long endTs = context.window().getEnd();
        String windowStart = DateFormatUtils.format(startTs, "yyyy-MM-dd HH:mm:ss.SSS");
        String windowEnd = DateFormatUtils.format(endTs, "yyyy-MM-dd HH:mm:ss.SSS");


        Map<String, String> map = new HashMap<>();
        map.put("appId", key.f0);
        map.put("apiId", key.f1);
        map.put("totalCount", String.valueOf(totalCount));
        map.put("errorCount", String.valueOf(errorCount));
        map.put("successRate", String.valueOf(successRate));
        map.put("windowStart", windowStart);
        map.put("windowEnd", windowEnd);

        collector.collect(JacksonUtils.toJsonNode(map));
    }
}
