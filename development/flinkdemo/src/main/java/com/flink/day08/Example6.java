package com.flink.day08;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

//连续三次登录失败检测
public class Example6 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<LoginEvent> stream = env.fromElements(
                new LoginEvent("user-1", "fail", "0.0.0.1", 1000L),
                new LoginEvent("user-1", "fail", "0.0.0.2", 2000L),
                new LoginEvent("user-2", "success", "0.0.0.3", 3000L),
                new LoginEvent("user-1", "fail", "0.0.0.4", 4000L)
        )
                .assignTimestampsAndWatermarks(WatermarkStrategy.<LoginEvent>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                            @Override
                            public long extractTimestamp(LoginEvent loginEvent, long l) {
                                return loginEvent.timestamp;
                            }
                        }));

        //定义模板
        //next表示挨着
        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("failed")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return loginEvent.type.equals("fail");
                    }
                })
                .times(3) // 连续三个事件
                .consecutive(); // 要求连续三个事件紧挨着

        //在流上匹配符合模板的事件组
        //匹配结果是事件的流
        PatternStream<LoginEvent> patternStream = CEP.pattern(stream.keyBy(r -> r.userId), pattern);

        //将匹配好的流中的事件组提取出来
        patternStream.select(new PatternSelectFunction<LoginEvent, String>() {
            @Override
            public String select(Map<String, List<LoginEvent>> map) throws Exception {
                LoginEvent first = map.get("failed").get(0);
                LoginEvent second = map.get("failed").get(1);
                LoginEvent third = map.get("failed").get(2);
                return "用户" + first.userId + "连续三次登录失败,ip是：" + first.ipAddr +";" + second.ipAddr +";" + third.ipAddr  ;
            }
        }).print();



        env.execute();
    }
    public static class LoginEvent{
        public String userId;
        public String type;
        public String ipAddr;
        public Long timestamp;

        public LoginEvent() {
        }

        public LoginEvent(String userId, String type, String ipAddr, Long timestamp) {
            this.userId = userId;
            this.type = type;
            this.ipAddr = ipAddr;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return "LoginEvent{" +
                    "userId='" + userId + '\'' +
                    ", type='" + type + '\'' +
                    ", ipAddr='" + ipAddr + '\'' +
                    ", timestamp=" + new Timestamp(timestamp) +
                    '}';
        }
    }
}
