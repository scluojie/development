package com.flink.day09;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

public class Example2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Example1.OrderEvent> stream = env
                .addSource(new SourceFunction<Example1.OrderEvent>() {
                    @Override
                    public void run(SourceContext<Example1.OrderEvent> ctx) throws Exception {
                        Example1.OrderEvent e1 = new Example1.OrderEvent("order-1", "create", 1000L);
                        ctx.collectWithTimestamp(e1, e1.timestamp);
                        Thread.sleep(1000L);
                        Example1.OrderEvent e2 = new Example1.OrderEvent("order-2", "create", 2000L);
                        ctx.collectWithTimestamp(e2, e2.timestamp);
                        Thread.sleep(1000L);
                        Example1.OrderEvent e3 = new Example1.OrderEvent("order-1", "pay", 5000L);
                        ctx.collectWithTimestamp(e3, e3.timestamp);
                        Thread.sleep(1000L);
                        ctx.emitWatermark(new Watermark(7000L));
                        Thread.sleep(1000L);
                        Example1.OrderEvent e4 = new Example1.OrderEvent("order-2", "pay", 8000L);
                        ctx.collectWithTimestamp(e4, e4.timestamp);
                        Thread.sleep(1000L);
                    }

                    @Override
                    public void cancel() {

                    }
                });

        stream
                .keyBy(r -> r.orderId)
                .process(new KeyedProcessFunction<String, Example1.OrderEvent, String>() {
                    private ValueState<Example1.OrderEvent> orderState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        orderState = getRuntimeContext().getState(new ValueStateDescriptor<Example1.OrderEvent>("order-state", Types.POJO(Example1.OrderEvent.class)));
                    }

                    @Override
                    public void processElement(Example1.OrderEvent value, Context ctx, Collector<String> out) throws Exception {
                        if(value.type.equals("create")){
                            //create 到达
                            orderState.update(value);
                            ctx.timerService().registerEventTimeTimer(value.timestamp + 5000L);
                        }else if(orderState.value()!= null & value.type.equals("pay") ){
                            out.collect("订单" + value.orderId + "支付成功");
                            orderState.clear();
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        if(orderState.value() != null && orderState.value().type.equals("create")){
                            out.collect("订单" + orderState.value().orderId + "支付超时");
                            orderState.clear();
                        }
                    }
                })
                .print();
        env.execute();
    }
}
