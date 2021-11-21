package com.flink.day09;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

public class Example1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<OrderEvent> stream = env
                .addSource(new SourceFunction<OrderEvent>() {

                    @Override
                    public void run(SourceContext<OrderEvent> ctx) throws Exception {
                        OrderEvent e1 = new OrderEvent("order-1", "create", 1000L);
                        ctx.collectWithTimestamp(e1, e1.timestamp);
                        Thread.sleep(1000L);
                        OrderEvent e2 = new OrderEvent("order-2", "create", 2000L);
                        ctx.collectWithTimestamp(e2, e2.timestamp);
                        Thread.sleep(1000L);
                        OrderEvent e3 = new OrderEvent("order-1", "pay", 5000L);
                        ctx.collectWithTimestamp(e3, e3.timestamp);
                        Thread.sleep(1000L);
                        ctx.emitWatermark(new Watermark(7000L));
                        Thread.sleep(1000L);
                        OrderEvent e4 = new OrderEvent("order-2", "pay", 8000L);
                        ctx.collectWithTimestamp(e4, e4.timestamp);
                        Thread.sleep(1000L);
                    }

                    @Override
                    public void cancel() {

                    }
                });

        //定义模板
        Pattern<OrderEvent, OrderEvent> pattern = Pattern.<OrderEvent>begin("create")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent orderEvent) throws Exception {
                        return orderEvent.type.equals("create");
                    }
                })
                .next("pay")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent orderEvent) throws Exception {
                        return orderEvent.type.equals("pay");
                    }
                })//要求两个事件在5秒钟之内发生
                .within(Time.seconds(5));

        PatternStream<OrderEvent> patternStream = CEP.pattern(stream.keyBy(r -> r.orderId), pattern);

        SingleOutputStreamOperator<String> result = patternStream.flatSelect(
                //侧输出流用来接收超时信息
                new OutputTag<String>("timeout-order") {
                },
                //第二个参数用来处理支付超市的订单
                new PatternFlatTimeoutFunction<OrderEvent, String>() {
                    @Override
                    public void timeout(Map<String, List<OrderEvent>> pattern, long l, Collector<String> out) throws Exception {
                        //超时事件指挥有一个事件，就是create 事件
                        OrderEvent create = pattern.get("create").get(0);
                        //将超时信息发送到侧输出流
                        out.collect("订单" + create.orderId + "支付超时");
                    }
                },
                //第三个参数，用来处理正常支付的订单
                new PatternFlatSelectFunction<OrderEvent, String>() {
                    @Override
                    public void flatSelect(Map<String, List<OrderEvent>> pattern, Collector<String> out) throws Exception {
                        //由于处理的是正常支付的订单 所以存在pay事件
                        OrderEvent pay = pattern.get("pay").get(0);
                        //直接向下游发送消息
                        out.collect("订单" + pay.orderId + "支付成功");
                    }
                }
        );

        result.print();

        result.getSideOutput(new OutputTag<String>("timeout-order"){}).print();
        env.execute();
    }

    public static class OrderEvent {
        public String orderId;
        public String type;
        public Long timestamp;

        public OrderEvent() {
        }

        public OrderEvent(String orderId, String type, Long timestamp) {
            this.orderId = orderId;
            this.type = type;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return "OrderEvent{" +
                    "orderId='" + orderId + '\'' +
                    ", type='" + type + '\'' +
                    ", timestamp=" + new Timestamp(timestamp) +
                    '}';
        }
    }}
