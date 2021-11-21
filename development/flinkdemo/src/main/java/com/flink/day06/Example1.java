package com.flink.day06;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;


//使用迟到事件来更新窗口计算结果
public class Example1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<String> result = env
                .socketTextStream("hadoop102", 9999)
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        String[] arr = value.split(" ");
                        return Tuple2.of(arr[0], Long.parseLong(arr[1]) * 1000L);
                    }
                })
                .assignTimestampsAndWatermarks(
                        //最大延迟时间设置为5秒钟
                        WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {

                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> element, long l) {
                                        return element.f1;
                                    }
                                })
                )
                .keyBy(r -> r.f0)
                //滚动窗口大小为5秒钟
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                //等待迟到数据5秒钟
                .allowedLateness(Time.seconds(5))
                .sideOutputLateData(new OutputTag<Tuple2<String, Long>>("late-event") {})
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {


                    @Override
                    public void process(String s, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                        //初始化一个当前窗口可见的状态变量
                        //用来作为标志为：是否是窗口第一次触发计算
                        ValueState<Boolean> isFirstCalculate = context.windowState().getState(
                                new ValueStateDescriptor<Boolean>("is-first-calculate", Types.BOOLEAN)
                        );

                        if (isFirstCalculate.value() == null) {
                            //当水位线达到窗口结束时间时，窗口第一次触发计算
                            out.collect("窗口第一次触发计算，共：" + elements.spliterator().getExactSizeIfKnown());
                            isFirstCalculate.update(true);

                        } else {
                            //迟到元素到来，走入当前条件分支
                            out.collect("迟到元素来了，共：" + elements.spliterator().getExactSizeIfKnown());
                        }
                    }
                });

        result.print("正常数据：");

        result.getSideOutput(new OutputTag<Tuple2<String,Long>>("late-event"){}).print("迟到数据：");


        env.execute();
    }
}
