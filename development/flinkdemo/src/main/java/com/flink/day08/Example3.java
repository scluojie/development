package com.flink.day08;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

//基于间隔的join
public class Example3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple3<String, String, Long>> buyStream = env.fromElements(
                Tuple3.of("user-1", "buy", 20 * 60 * 1000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple3<String, String, Long> element, long l) {
                                return element.f2;
                            }
                        }));

        SingleOutputStreamOperator<Tuple3<String, String, Long>> pvStream = env.fromElements(
                Tuple3.of("user-1", "pv", 9 * 60 * 1000L),
                Tuple3.of("user-1", "pv", 10 * 60 * 1000L),
                Tuple3.of("user-1", "pv", 12 * 60 * 1000L),
                Tuple3.of("user-1", "pv", 15 * 60 * 1000L),
                Tuple3.of("user-1", "pv", 21 * 60 * 1000L)
                )
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple3<String, String, Long> element, long l) {
                                return element.f2;
                            }
                        }));

        buyStream.keyBy(r ->r.f0)
                .intervalJoin(pvStream.keyBy(r->r.f0))
                .between(Time.minutes(-10),Time.minutes(5))
                .process(new ProcessJoinFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>() {
                    @Override
                    public void processElement(Tuple3<String, String, Long> left, Tuple3<String, String, Long> right, Context ctx, Collector<String> out) throws Exception {
                        out.collect(left + " =>" +right);
                    }
                })
                .print("buy => pv");

        pvStream.keyBy(r -> r.f0)
                .intervalJoin(buyStream.keyBy(r -> r.f0))
                .between(Time.minutes(-5), Time.minutes(10))
                .process(new ProcessJoinFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>() {
                    @Override
                    public void processElement(Tuple3<String, String, Long> left, Tuple3<String, String, Long> right, Context ctx, Collector<String> out) throws Exception {
                        out.collect(right + " => " + left);
                    }
                })
                .print("pv => buy: ");

        // a.ts + low < b.ts < a.ts + high
        // b.ts + (-high) < a.ts < b.ts + (-low)

        env.execute();
    }
}
