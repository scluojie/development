package com.flink.day08;

import com.flink.day03.Example8;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

//开关流
public class Example2 {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Example8.SensorReading> sensorStream = env.addSource(new Example8.SensorSource());

        DataStreamSource<Tuple2<String, Long>> switchStream = env.fromElements(Tuple2.of("sensor_2", 10 * 1000L));
        
        sensorStream.keyBy(r -> r.id)
                .connect(switchStream.keyBy(r->r.f0))
                .process(new CoProcessFunction<Example8.SensorReading, Tuple2<String, Long>, Example8.SensorReading>() {

                    //开关状态变量
                    private ValueState<Boolean> forwarding;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        forwarding = getRuntimeContext().getState(
                                new ValueStateDescriptor<Boolean>(
                                        "switch", Types.BOOLEAN
                                )
                        );
                    }

                    @Override
                    public void processElement1(Example8.SensorReading value, Context ctx, Collector<Example8.SensorReading> out) throws Exception {
                          //如果开关为开的情况下，放行数据
                        if(forwarding.value() != null && forwarding.value()){
                            out.collect(value);
                        }
                    }

                    @Override
                    public void processElement2(Tuple2<String, Long> value, Context ctx, Collector<Example8.SensorReading> out) throws Exception {
                        //打开开关
                        forwarding.update(true);
                        //注册关闭开关的定时器
                        ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime()+ value.f1);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Example8.SensorReading> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        //定时器用来关闭开关
                        forwarding.clear();
                    }
                })
                .print();

        env.execute();
    }
}
