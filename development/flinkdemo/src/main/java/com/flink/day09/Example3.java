package com.flink.day09;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
//将数据流转换成动态表，然后再转换回数据流并打印
public class Example3 {
    public static void main(String[] args) throws Exception {
        // 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 数据源，且设置水位线
        SingleOutputStreamOperator<Tuple3<String, String, Long>> stream = env
                .fromElements(
                        Tuple3.of("Mary", "./home", 12 * 60 * 60 * 1000L),
                        Tuple3.of("Bob", "./cart", 12 * 60 * 60 * 1000L),
                        Tuple3.of("Mary", "./prod?id=1", 12 * 60 * 60 * 1000L + 5 * 1000L),
                        Tuple3.of("Liz", "./home", 12 * 60 * 60 * 1000L + 60 * 1000L),
                        Tuple3.of("Bob", "./prod?id=3", 12 * 60 * 60 * 1000L + 90 * 1000L),
                        Tuple3.of("Mary", "./prod?id=7", 12 * 60 * 60 * 1000L + 105 * 1000L)
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

        // 获取动态表环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env, settings);

        // 将数据流转换成动态表
        Table table = streamTableEnvironment
                .fromDataStream(
                        stream,
                        // 默认字段名是f0，命名一个别名为user
                        $("f0").as("user"),
                        $("f1").as("url"),
                        // rowtime表示告诉flink sql，f2字段是事件时间
                        $("f2").rowtime().as("cTime")
                );

        // 将动态表转换回数据流
        // Row表示行的泛型，万能泛型
        DataStream<Row> rowDataStream = streamTableEnvironment.toDataStream(table);

        // +I表示insert，向动态表中插入数据
        rowDataStream.print();

        env.execute();
    }
}
