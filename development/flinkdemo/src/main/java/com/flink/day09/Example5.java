package com.flink.day09;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class Example5 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple3<String, String, Long>> stream = env
                .fromElements(
                        Tuple3.of("Mary", "./home", 12 * 60 * 60 * 1000L),
                        Tuple3.of("Bob", "./cart", 12 * 60 * 60 * 1000L),
                        Tuple3.of("Mary", "./prod?id=1", 12 * 60 * 60 * 1000L + 2 * 60 * 1000L),
                        Tuple3.of("Mary", "./prod?id=4", 12 * 60 * 60 * 1000L + 55 * 60 * 1000L),
                        Tuple3.of("Bob", "./prod?id=5", 13 * 60 * 60 * 1000L + 60 * 1000L),
                        Tuple3.of("Liz", "./home", 13 * 60 * 60 * 1000L + 30 * 60 * 1000L),
                        Tuple3.of("Liz", "./prod?id=7", 13 * 60 * 60 * 1000L + 59 * 60 * 1000L),
                        Tuple3.of("Mary", "./cart", 14 * 60 * 60 * 1000L),
                        Tuple3.of("Liz", "./home", 14 * 60 * 60 * 1000L + 2 * 60 * 1000L),
                        Tuple3.of("Bob", "./prod?id=3", 14 * 60 * 60 * 1000L + 30 * 60 * 1000L),
                        Tuple3.of("Bob", "./home", 14 * 60 * 60 * 1000L + 40 * 60 * 1000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                                        return element.f2;
                                    }
                                })
                );

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env, settings);

        Table table = streamTableEnvironment.fromDataStream(
                stream,
                $("f0").as("user"),
                $("f1").as("url"),
                $("f2").rowtime().as("cTime")
        );

        streamTableEnvironment.createTemporaryView("clicks",table);

        //Tumble 定义了滚动窗口 第一个参数是指明了事件戳 第二个参数是滚动窗口长度
        Table result = streamTableEnvironment.sqlQuery(
                "SELECT user,COUNT(url) as cnt, TUMBLE_END(cTime, INTERVAL '1' HOUR ) AS windowEnd from clicks group by user,TUMBLE(cTime,INTERVAL '1' HOUR)"
        );

        streamTableEnvironment.toChangelogStream(result).print();

        env.execute();
    }
}
