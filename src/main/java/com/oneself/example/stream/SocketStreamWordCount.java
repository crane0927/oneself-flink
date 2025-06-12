package com.oneself.demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author liuhuan
 * date 2025/1/8
 * packageName com.oneself.demo
 * className SocketStreamWordCount
 * description socket 流处理 文字统计（无界数据）
 * version 1.0
 */
public class SocketStreamWordCount {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 从 socket 读取数据
        // 终端执行命令：nc -lk 7777 输入数据
        DataStreamSource<String> lineStream = env.socketTextStream("127.0.0.1", 7777);

        // 3. 转换计算：分组、求和获得统计结果
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = lineStream.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String line, Collector<Tuple2<String, Long>> collector) throws Exception {
                        String[] words = line.split(" ");
                        for (String word : words) {
                            collector.collect(Tuple2.of(word, 1L));
                        }
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(data -> data.f0)
                .sum(1);

        // 4. 输出结果
        sum.print();
        // 5. 启动执行环境
        env.execute();
    }
}
