package com.flink.day09;


import com.flink.day04.Example10;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

///使用flink sql 实现实时热门商品
public class Example6 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Example10.UserBehavior> stream = env.readTextFile("D:\\development\\flinkdemo\\src\\main\\resources\\UserBehavior.csv")
                .map(new MapFunction<String, Example10.UserBehavior>() {
                    @Override
                    public Example10.UserBehavior map(String s) throws Exception {
                        String[] arr = s.split(",");
                        return new Example10.UserBehavior(
                                arr[0], arr[1], arr[2], arr[3],
                                Long.parseLong(arr[4]) * 1000L
                        );
                    }
                })
                .filter(r -> r.behavior.equals("pv"))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Example10.UserBehavior>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Example10.UserBehavior>() {
                            @Override
                            public long extractTimestamp(Example10.UserBehavior userBehavior, long l) {
                                return userBehavior.timestamp;
                            }
                        }));

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env, settings);

        Table table = streamTableEnvironment.fromDataStream(
                stream,
                $("itemId"),
                //timestamp是sql中的关键字 所以命名别名为ts
                $("timestamp").rowtime().as("ts")
        );

        streamTableEnvironment.createTemporaryView("userbehavior",table);
        String innerSQL = "select itemId,count(itemId) as cnt, " +
                "HOP_START(ts,INTERVAL '5' MINUTES,INTERVAL '1' HOURS) as windowStart, " +
                "HOP_END(ts,INTERVAL '5' MINUTES,INTERVAL '1' HOURS) as windowEnd " +
                "from userbehavior group by itemId,HOP(ts,INTERVAL '5' MINUTES,INTERVAL '1' HOURS)";


        //取出前三名
        String outerSQL = "select * from (" + "select * ,row_number() over(partition by windowEnd order by cnt desc) as row_num from " +
                "(" + innerSQL + "))" + "where row_num <= 3";

        System.out.println(outerSQL);

        Table result = streamTableEnvironment.sqlQuery(outerSQL);

        streamTableEnvironment.toChangelogStream(result).print();

        env.execute();
    }
}
