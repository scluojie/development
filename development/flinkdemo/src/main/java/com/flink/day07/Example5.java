package com.flink.day07;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.HashSet;

//计算独立访客
public class Example5 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .readTextFile("D:\\development\\flinkdemo\\src\\main\\resources\\UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String userBehavior) throws Exception {
                        String[] arr = userBehavior.split(",");
                        return new UserBehavior(
                                arr[0],
                                arr[1],
                                arr[2],
                                arr[3],
                                Long.parseLong(arr[4]) * 1000L
                        );
                    }
                })
                .filter(userBehavior -> userBehavior.behavior.equals("pv"))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                    @Override
                    public long extractTimestamp(UserBehavior userBehavior, long l) {
                        return userBehavior.timestamp;
                    }
                }))
                .keyBy(userBehavior -> true)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new CountAgg(),new WindowResult())
                .print();

        env.execute();

    }

    public static class WindowResult extends ProcessWindowFunction<Long,String,Boolean, TimeWindow>{

        @Override
        public void process(Boolean aBoolean, Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
            Long count = elements.iterator().next();
            long windowStart = context.window().getStart();
            long windowEnd = context.window().getEnd();
            out.collect("窗口" + new Timestamp(windowStart) + "~" + new Timestamp(windowEnd) + "的UV是：" + count);
        }
    }
    public static class CountAgg implements AggregateFunction<UserBehavior, HashSet<String>,Long>{


        @Override
        public HashSet<String> createAccumulator() {
            return new HashSet<>();
        }

        @Override
        public HashSet<String> add(UserBehavior userBehavior, HashSet<String> accumulator) {
            accumulator.add(userBehavior.userId);
            return accumulator;
        }

        @Override
        public Long getResult(HashSet<String> accumulator) {
            return (long)accumulator.size();
        }

        @Override
        public HashSet<String> merge(HashSet<String> strings, HashSet<String> acc1) {
            return null;
        }
    }
    public static class UserBehavior{
        public String userId;
        public String itemId;
        public String categoryId;
        public String behavior;
        public Long timestamp;

        public UserBehavior() {
        }

        public UserBehavior(String userId, String itemId, String categoryId, String behavior, Long timestamp) {
            this.userId = userId;
            this.itemId = itemId;
            this.categoryId = categoryId;
            this.behavior = behavior;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return "UserBehavior{" +
                    "userId='" + userId + '\'' +
                    ", itemId='" + itemId + '\'' +
                    ", categoryId='" + categoryId + '\'' +
                    ", behavior='" + behavior + '\'' +
                    ", timestamp=" + new Timestamp(timestamp) +
                    '}';
        }
    }
}
