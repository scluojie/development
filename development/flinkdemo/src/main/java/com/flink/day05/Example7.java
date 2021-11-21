package com.flink.day05;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

//将迟到元素路由到侧输出流中去
public class Example7 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<String> result = env.addSource(new SourceFunction<Tuple2<String, Long>>() {
            @Override
            public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
                ctx.collectWithTimestamp(Tuple2.of("a", 1000L), 1000L);
                ctx.collectWithTimestamp(Tuple2.of("a", 2000L), 2000L);
                ctx.emitWatermark(new Watermark(4999L));
                ctx.collectWithTimestamp(Tuple2.of("a", 1500L), 1500L);
            }

            @Override
            public void cancel() {

            }
        })
                .keyBy(r -> r.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                //侧输出流中的元素类型必须要和主流只的一样
                .sideOutputLateData(new OutputTag<Tuple2<String, Long>>("late-event") {
                })
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                        out.collect("窗口中共有：" + elements.spliterator().getExactSizeIfKnown());
                    }
                });

        result.print("正常数据：");

        result.getSideOutput(new OutputTag<Tuple2<String,Long>>("late-event"){}).print("迟到数据：");


        env.execute();
    }
}
