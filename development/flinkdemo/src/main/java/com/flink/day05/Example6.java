package com.flink.day05;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

//将迟到的元素路由到侧输出流中去
public class Example6 {
    //定义侧输出流的名字标签
    //侧输出流中的元素类型是String
    //侧输出标签也是单例
    private static OutputTag<String> lateEvent = new OutputTag<String>("late-event"){};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<String> result = env.addSource(new SourceFunction<Tuple2<String, Long>>() {

            @Override
            public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
                ctx.collectWithTimestamp(Tuple2.of("a", 1000L), 1000L);
                ctx.collectWithTimestamp(Tuple2.of("a", 2000L), 2000L);
                ctx.emitWatermark(new Watermark(1999L));
                ctx.collectWithTimestamp(Tuple2.of("a", 1500L), 1500L);
            }

            @Override
            public void cancel() {

            }
        })
                .keyBy(r -> r.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, Long>, String>() {

                    @Override
                    public void processElement(Tuple2<String, Long> value, Context ctx, Collector<String> out) throws Exception {
                        if (value.f1 < ctx.timerService().currentWatermark()) {
                            //将迟到元素发送到侧输出流中去
                            ctx.output(lateEvent, "迟到数据来了：" + value);
                        } else {
                            out.collect("正常数据到达：" + value);
                        }
                    }
                });
        //打印主流数据
        result.print("正常到达>");
        //打印侧输出流的数据
        result.getSideOutput(lateEvent).print("迟到数据>");

        env.execute();
    }
}
