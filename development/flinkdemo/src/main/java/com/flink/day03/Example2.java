package com.flink.day03;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

//增量聚合函数的使用
//AggregateFunction
public class Example2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();;

        env.setParallelism(1);

        env.addSource(new com.flink.day02.Example2.CustomSource())
                .keyBy(r -> r.user)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new CountAgg())
                .print();


        env.execute();
    }

    public static class CountAgg implements AggregateFunction<com.flink.day02.Example2.Event, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L; // 创建累加器
        }

        @Override
        public Long add(com.flink.day02.Example2.Event value, Long accumulator) {
            return accumulator + 1L; // 输入元素和累加器的累加的逻辑
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator; // 窗口闭合时输出什么结果
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }
}
