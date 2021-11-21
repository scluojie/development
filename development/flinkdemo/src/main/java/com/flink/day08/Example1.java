package com.flink.day08;

import javafx.util.Duration;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

//实时对账
public class Example1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<OrderEvent> appZhifuStream = env.addSource(new SourceFunction<OrderEvent>() {
            @Override
            public void run(SourceContext<OrderEvent> ctx) throws Exception {
                OrderEvent e1 = new OrderEvent("order-1", "app-zhifu", 1000L);
                ctx.collectWithTimestamp(e1, e1.timestamp);
                OrderEvent e2 = new OrderEvent("order-2", "app-zhufu", 2000L);
                ctx.collectWithTimestamp(e2, e2.timestamp);
            }

            @Override
            public void cancel() {

            }
        });


        DataStreamSource<OrderEvent> weixinZhifuStream = env.addSource(new SourceFunction<OrderEvent>() {
            @Override
            public void run(SourceContext<OrderEvent> ctx) throws Exception {
                OrderEvent e1 = new OrderEvent("order-1", "weixin-zhifu", 4000L);
                ctx.collectWithTimestamp(e1, e1.timestamp);
                OrderEvent e2 = new OrderEvent("order-3", "weixin-zhifu", 6000L);
                ctx.collectWithTimestamp(e2, e2.timestamp);
                //水位线此时为1999 因为有5秒定时
                //把水位线拉高
                ctx.emitWatermark(new Watermark(7000L));
                OrderEvent e3 = new OrderEvent("order-2", "weixin-zhifu", 8000L);
                ctx.collectWithTimestamp(e3,e3.timestamp);
            }

            @Override
            public void cancel() {

            }
        });

        appZhifuStream.keyBy(r->r.orderId)
                .connect(weixinZhifuStream.keyBy(r -> r.orderId))
                .process(new MatchFunction())
                .print();

        env.execute();
    }

    public static class MatchFunction extends CoProcessFunction<OrderEvent, OrderEvent, String> {
        //如果app支付信息先到达，那么保存在appEvent中，并等待微信支付的到达5秒钟
        private ValueState<OrderEvent> appEvent;
        //如果weixin支付信息先到达，那么保存在weixinEvent中，并等待app支付的到达5秒钟
        private ValueState<OrderEvent> weixinEvent;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            //初始化状态变量
           appEvent =  getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("app", Types.POJO(OrderEvent.class)));
           weixinEvent = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("weixin",Types.POJO(OrderEvent.class)));
        }

        @Override
        public void processElement1(OrderEvent value, Context ctx, Collector<String> out) throws Exception {
            //weixin支付先到达
            if(weixinEvent.value() != null){
                out.collect("app " + value.orderId + " 对账成功");
                weixinEvent.clear();
            }
            //app 支付先到达
            else {
                //先把appEvent 存储起来
                appEvent.update(value);
                //注册5秒定时器 等待weixin支付
                ctx.timerService().registerEventTimeTimer(value.timestamp +  5000L);

            }
        }

        @Override
        public void processElement2(OrderEvent value, Context ctx, Collector<String> out) throws Exception {
            if(appEvent.value() != null){
                //app支付先到达
                out.collect("weixin " + value.orderId + " 对账成功");
                appEvent.clear();
            }else{
                //weixin支付先到达
                //保存weixin 支付信息
                weixinEvent.update(value);
                //注册5秒后的一个定时器
                ctx.timerService().registerEventTimeTimer(value.timestamp + 5000L);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            if(appEvent.value()!= null){
                //对应weixin支付没来
                out.collect("app " + appEvent.value().orderId + " 对账失败");
                appEvent.clear();
            }
            if(weixinEvent.value() != null){
                //对应app支付没来
                out.collect("weixin " + weixinEvent.value().orderId + "对账失败");
                weixinEvent.clear();
            }
        }
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
    }
}
