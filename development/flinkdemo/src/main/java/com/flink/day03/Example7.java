package com.flink.day03;


import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

//ValueState的使用
public class Example7 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new SourceFunction<Integer>() {
                    private boolean running = true;
                    private Random random = new Random();
                    @Override
                    public void run(SourceContext<Integer> ctx) throws Exception {
                        while (running) {
                            ctx.collect(random.nextInt(10000));
                            Thread.sleep(10L);
                        }
                    }

                    @Override
                    public void cancel() {
                        running = false;
                    }
                })
                .keyBy(r -> true)
                .process(new KeyedProcessFunction<Boolean, Integer, Double>() {
                    // 声明一个值状态变量
                    // 状态变量的作用域是当前key，每一条支流都会维护自己的状态变量
                    // 状态变量是单例
                    private ValueState<Tuple2<Integer, Integer>> accumulator;

                    // 用来存储发送平均值的定时器的时间戳
                    private ValueState<Long> timerTs;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        // 实例化状态变量，里面的值的初始值是null
                        accumulator = getRuntimeContext().getState(
                                new ValueStateDescriptor<Tuple2<Integer, Integer>>(
                                        "acc", // 字符串名字保证单例特性
                                        Types.TUPLE(Types.INT, Types.INT)
                                )
                        );

                        timerTs = getRuntimeContext().getState(
                                new ValueStateDescriptor<Long>("timer", Types.LONG)
                        );
                    }

                    @Override
                    public void processElement(Integer value, Context ctx, Collector<Double> out) throws Exception {
                        // 当第一条数据到来的时候，状态变量为null
                        if (accumulator.value() == null) {
                            accumulator.update(Tuple2.of(value, 1));
                        } else {
                            Tuple2<Integer, Integer> temp = accumulator.value(); // 读取状态变量中的值
                            accumulator.update(Tuple2.of(temp.f0 + value, temp.f1 + 1)); // 更新状态变量
                        }
                        if (timerTs.value() == null) {
                            // 注册一个发送平均值的定时器
                            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 10 * 1000L);
                            timerTs.update(ctx.timerService().currentProcessingTime() + 10 * 1000L);
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Double> out) throws Exception {
                        // 定时器用来发送平均值
                        super.onTimer(timestamp, ctx, out);
                        out.collect((double) accumulator.value().f0 / accumulator.value().f1);
                        // 发送完平均值以后，清空状态变量
                        timerTs.clear();
                    }
                })
                .print();

        env.execute();
    }
}
