package com.flink.day05;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;

//使用MapState实现增量聚合函数和全窗口聚合函数的结合使用
//开的是1小时的滚动窗口
public class Example5 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .readTextFile("D:\\development\\flinkdemo\\src\\main\\resources\\UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String s) throws Exception {
                        String[] arr = s.split(",");

                        return new UserBehavior(
                                arr[0],
                                arr[1],
                                arr[2],
                                arr[3],
                                Long.parseLong(arr[4]) * 1000L
                        );
                    }
                })
                .filter(r -> r.behavior.equals("pv"))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>(){

                            @Override
                            public long extractTimestamp(UserBehavior userBehavior, long l) {
                                return userBehavior.timestamp;

                            }
                        })
                        )
                .keyBy(r ->r.itemId)
                .process(new FakeWindow(1L))
                .keyBy(r -> r.windowEnd)
                .process(new TopN(3))
                .print();

        env.execute();
    }

    public static class TopN extends KeyedProcessFunction<Long, ItemViewCount, String> {
        private int n;

        public TopN(int n) {
            this.n = n;
        }

        // 初始化一个列表状态变量，用来保存ItemViewCount
        private ListState<ItemViewCount> listState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            listState = getRuntimeContext().getListState(
                    new ListStateDescriptor<ItemViewCount>("list-state", Types.POJO(ItemViewCount.class))
            );
        }

        @Override
        public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
            // 每来一个ItemViewCount就存入列表状态变量
            listState.add(value);
            // 不会重复注册定时器
            // 定时器用来排序，因为可以确定所有ItemViewCount都到了
            ctx.timerService().registerEventTimeTimer(value.windowEnd + 1L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            // 将数据取出排序
            ArrayList<ItemViewCount> itemViewCounts = new ArrayList<>();
            for (ItemViewCount ivc : listState.get()) {
                itemViewCounts.add(ivc);
            }
            listState.clear();

            // 按照浏览量降序排列
            itemViewCounts.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount t2, ItemViewCount t1) {
                    return t1.count.intValue() - t2.count.intValue();
                }
            });

            StringBuilder result = new StringBuilder();
            result
                    .append("==================================================\n")
                    .append("窗口结束时间：" + new Timestamp(timestamp - 1L))
                    .append("\n");

            for (int i = 0; i < n; i++) {
                ItemViewCount currIvc = itemViewCounts.get(i);
                result
                        .append("第" + (i+1) + "名的商品ID是：" + currIvc.itemId + "，浏览量是：" + currIvc.count + "\n");
            }
            result
                    .append("==================================================\n");
            out.collect(result.toString());
        }
    }

    public  static  class FakeWindow extends KeyedProcessFunction<String,UserBehavior,ItemViewCount>{
        //key: windowStart
        //value:count
        private MapState<Long,Long> mapState;

        private Long windowSize;

        public FakeWindow(Long windowSize) {
            this.windowSize = windowSize * 60 * 60 * 1000L;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            mapState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<Long, Long>(
                            "windowStart-count",
                            Types.LONG,
                            Types.LONG
                    )
            );
        }

        @Override
        public void processElement(UserBehavior value, Context ctx, Collector<ItemViewCount> out) throws Exception {
                 long windowStart = value.timestamp -value.timestamp % windowSize;
                 long windowEnd = windowStart + windowSize;
                 if(mapState.contains(windowStart)){
                     mapState.put(windowStart, mapState.get(windowStart) + 1L);
                 }else{
                     mapState.put(windowStart,1L);
                 }
                 ctx.timerService().registerEventTimeTimer(windowEnd - 1L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<ItemViewCount> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            long windowStart = timestamp + 1L -windowSize;
            long windowEnd = windowStart + windowSize;
            long count = mapState.get(windowStart);
            String itemId = ctx.getCurrentKey();
            out.collect(new ItemViewCount(itemId,count,windowStart,windowEnd));
            mapState.remove(windowStart);//销毁窗口
        }
    }

    public static class ItemViewCount {
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
    }

    public static class UserBehavior {
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

