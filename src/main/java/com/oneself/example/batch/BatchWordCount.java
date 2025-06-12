package com.oneself.example.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author liuhuan
 * date 2025/1/3
 * packageName com.oneself.example.batch
 * className BatchWordCount
 * description 文件信息批处理（有界数据）
 * version 1.0
 */
public class BatchWordCount {
    /**
     * 以空格分隔符分词，统计每个单词出现的次数。
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 从文件中读取数据
        DataStreamSource<String> lineDS = env.readTextFile("/Users/liuhuan/project/java/local/flink-demo/src/input/input.txt");

        // 3. 分组、聚合计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordCounts = lineDS
                .flatMap(new WordSplitter())
                .keyBy(t -> t.f0) // 按单词分组
                .sum(1); // 统计个数

        // 4. 输出结果
        wordCounts.print();


        // 5. 启动执行环境
        env.execute("Word Count Batch Job");
    }

    /**
     * 自定义 FlatMapFunction，用于将输入的行分词并输出 (单词, 1) 的二元组。
     */
    public static class WordSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) {
            // 按空格分词并收集结果
            for (String word : line.split(" ")) {
                collector.collect(Tuple2.of(word, 1));
            }
        }
    }
}