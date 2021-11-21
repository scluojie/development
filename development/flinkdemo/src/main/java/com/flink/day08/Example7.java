package com.flink.day08;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

//使用底层api来实现连续三次登录失败检测
public class Example7 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Example6.LoginEvent> stream = env.fromElements(
                new Example6.LoginEvent("user-1", "fail", "0.0.0.1", 1000L),
                new Example6.LoginEvent("user-1", "fail", "0.0.0.2", 2000L),
                new Example6.LoginEvent("user-2", "success", "0.0.0.3", 3000L),
                new Example6.LoginEvent("user-1", "fail", "0.0.0.4", 4000L)
        )
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Example6.LoginEvent>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Example6.LoginEvent>() {
                            @Override
                            public long extractTimestamp(Example6.LoginEvent element, long l) {
                                return element.timestamp;
                            }
                        }));


        stream.keyBy(r-> r.userId)
                .process(new KeyedProcessFunction<String, Example6.LoginEvent, String>() {
                    //用来保存状态转移矩阵
                    private HashMap<Tuple2<String,String>,String> stateMachine = new HashMap<>();

                    //保存当前状态
                    private ValueState<String> currentState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        //INITIAL 状态接受到success事件，跳转到SUCCESS
                        stateMachine.put(Tuple2.of("INITIAL","success"),"SUCCESS");
                        stateMachine.put(Tuple2.of("INITIAL","fail"),"S1");
                        stateMachine.put(Tuple2.of("S1","fail"),"S2");
                        stateMachine.put(Tuple2.of("S1","success"),"SUCCESS");
                        stateMachine.put(Tuple2.of("S2","fail"),"FAIL");
                        stateMachine.put(Tuple2.of("S2","success"),"SUCCESS");

                        currentState = getRuntimeContext().getState(new ValueStateDescriptor<String>(
                               "current-state", Types.STRING
                        ));
                    }

                    @Override
                    public void processElement(Example6.LoginEvent value, Context ctx, Collector<String> out) throws Exception {
                        if(currentState.value() == null){
                            currentState.update("INITIAL");
                        }

                        //将要跳转到的状态
                        String nextState = stateMachine.get(Tuple2.of(currentState.value(), value.type));
                        if(nextState.equals("FAIL")){
                            currentState.update("S2");
                            out.collect("用户" + value.userId + "连接三次登录失败");
                        }else if(nextState.equals("SUCCESS")){
                            currentState.update("INITIAL");
                        }else{
                            currentState.update(nextState);
                        }
                    }
                })
                .print();
        env.execute();
    }
}
