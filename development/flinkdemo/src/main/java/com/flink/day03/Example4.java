package com.flink.day03;

import com.flink.day02.Example2;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Calendar;


public class Example4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.addSource(new Example2.CustomSource())
                .keyBy(r -> true)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new CountAgg())
                .print();

        env.execute();
    }
    public static class CountAgg implements AggregateFunction<Example2.Event, Tuple4<String,Long,Long,Long>, Example1.UserViewCountPerWindow>{

        @Override
        public Tuple4<String, Long, Long, Long> createAccumulator() {
            return Tuple4.of("",0L,0L,0L);
        }

        @Override
        public Tuple4<String, Long, Long, Long> add(Example2.Event value, Tuple4<String, Long, Long, Long> accumulator) {
            long currTs = Calendar.getInstance().getTimeInMillis();
            long windowStart = currTs - currTs % 5000;
            long windowEnd = windowStart + 5000L;
            long count = accumulator.f1 + 1L;
            String user = value.user;
            return Tuple4.of(user,count,windowStart,windowEnd);
        }

        @Override
        public Example1.UserViewCountPerWindow getResult(Tuple4<String, Long, Long, Long> accumulator) {
            return new Example1.UserViewCountPerWindow(
                    accumulator.f0,accumulator.f1,accumulator.f2,accumulator.f3
            );
        }

        @Override
        public Tuple4<String, Long, Long, Long> merge(Tuple4<String, Long, Long, Long> a, Tuple4<String, Long, Long, Long> b) {
            return null;
        }
    }
}

