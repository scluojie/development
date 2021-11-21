package com.flink.day04;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

//水位线测试
public class Example2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //每隔半分钟的机器时间插入一次水位线
        env.getConfig().setAutoWatermarkInterval(30 * 1000L);

        env
                .socketTextStream("hadoop102",9999)
                .map(r -> Tuple2.of(r.split(" ")[0],Long.parseLong(r.split(" ")[1]) * 1000L))
                .returns(Types.TUPLE(Types.STRING,Types.LONG))
                .assignTimestampsAndWatermarks(
                        // 最大延迟时间设置为5秒钟
                        WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                        return element.f1; // 告诉flink时间戳是哪一个字段
                                    }
                                })
                )
                .keyBy(r -> r.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                        long count = elements.spliterator().getExactSizeIfKnown();
                        String windowStart = new Timestamp(context.window().getStart()).toString();
                        String windowEnd = new Timestamp(context.window().getEnd()).toString();
                        out.collect(
                                "窗口：" + windowStart + "~" + windowEnd + "共有：" + count
                        );
                    }
                })
                .print();
        env.execute();
    }
}
