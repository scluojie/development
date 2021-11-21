package com.flink.day04;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

//实时topN 计算
public class Example10 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env
                .readTextFile("D:\\development\\flinkdemo\\src\\main\\resources\\UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {

                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] arr = value.split(",");
                        return new UserBehavior(
                                arr[0],arr[1],arr[2],arr[3],
                                Long.parseLong(arr[4]) * 1000L
                        );
                    }
                })
                .filter(r -> r.behavior.equals("pv"))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                            @Override
                            public long extractTimestamp(UserBehavior userBehavior, long l) {
                                return userBehavior.timestamp;
                            }
                        })
                )
                .keyBy(r -> r.itemId)
                .window(SlidingEventTimeWindows.of(Time.hours(1),Time.minutes(5)))
                .aggregate(new CountAgg(),new WindowResult())
                .keyBy(r ->r.windowEnd)
                .process(new TopN(3))
                .print();
        env.execute();

    }

    public static class TopN extends KeyedProcessFunction<Long, ItemViewCount, String> {
        private int n;

        public TopN(int n) {
            this.n = n;
        }

        //初始化一个列表状态变量 用来保存ItemViewCount
        private ListState<ItemViewCount> listState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            listState = getRuntimeContext().getListState(
                    new ListStateDescriptor<ItemViewCount>("list-state", Types.POJO(ItemViewCount.class))
            );
        }

        @Override
        public void processElement(ItemViewCount itemViewCount, Context context, Collector<String> collector) throws Exception {
            //每来一个ItemViewCount 就存入列表状态变量
            listState.add(itemViewCount);
            //不会重复注册定时器
            //定时器用来排序 因为可以确定所有ItemViewCount都到了
            context.timerService().registerEventTimeTimer(itemViewCount.windowEnd + 1L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            //将数据取出排序
            ArrayList<ItemViewCount> itemViewCounts = new ArrayList<>();
            for (ItemViewCount itemViewCount : listState.get()) {
                itemViewCounts.add(itemViewCount);
            }
            listState.clear();

            //按照浏览量降序排列
           itemViewCounts.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    return o2.count.intValue()-o1.count.intValue();
                }
            });

            //Collections.sort(itemViewCounts,new ItemViewCount());

            StringBuilder result = new StringBuilder();

            result
                    .append("==============================================\n")
                    .append("窗口结束时间：" + new Timestamp(timestamp - 1L))
                    .append("\n");

            for (int i = 0; i < n; i++) {
                ItemViewCount currIvc = itemViewCounts.get(i);
                result
                        .append("第" + (i + 1) + "名的商品ID是：" + currIvc.itemId + "浏览量是：" + currIvc.count + "\n");
            }
            out.collect(result.toString());
        }
    }

    public static class WindowResult extends ProcessWindowFunction<Long,ItemViewCount,String, TimeWindow>{


        @Override
        public void process(String s, Context context, Iterable<Long> elements, Collector<ItemViewCount> collector) throws Exception {
            collector.collect(
                    new ItemViewCount(
                            s,
                            elements.iterator().next(),
                            context.window().getStart(),
                            context.window().getEnd()
                    )
            );
        }
    }

    public static class ItemViewCount /*implements Comparator*/ {
        public String itemId;
        public Long count;
        public Long windowStart;
        public Long windowEnd;

        public ItemViewCount() {
        }

        public ItemViewCount(String itemId, Long count, Long windowStart, Long windowEnd) {
            this.itemId = itemId;
            this.count = count;
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
        }

        @Override
        public String toString() {
            return "ItemViewCount{" +
                    "itemId='" + itemId + '\'' +
                    ", count=" + count +
                    ", windowStart=" + new Timestamp(windowStart) +
                    ", windowEnd=" + new Timestamp(windowEnd) +
                    '}';
        }

        /*@Override
        public int compare(Object o1, Object o2) {
            return (int) (((ItemViewCount) o2).count- ((ItemViewCount)o1).count);
        }*/
    }

    public static class CountAgg implements AggregateFunction<UserBehavior,Long,Long>{

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior userBehavior, Long accumulator) {
            return accumulator + 1L;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return null;
        }
    }

    public static class UserBehavior{
        public String userid;
        public String itemId;
        public String categoryId;
        public String behavior;
        public Long timestamp;

        public UserBehavior() {
        }

        public UserBehavior(String userid, String itemId, String categoryId, String behavior, Long timestamp) {
            this.userid = userid;
            this.itemId = itemId;
            this.categoryId = categoryId;
            this.behavior = behavior;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return "UserBehavior{" +
                    "userid='" + userid + '\'' +
                    ", itemId='" + itemId + '\'' +
                    ", categoryId='" + categoryId + '\'' +
                    ", behavior='" + behavior + '\'' +
                    ", timestamp=" + new Timestamp(timestamp) +
                    '}';
        }
    }


}
