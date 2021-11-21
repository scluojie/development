package com.flink.day04;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

public class Example4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new SourceFunction<String>() {
            @Override
            public void run(SourceContext<String> sourceContext) throws Exception {
                sourceContext.collectWithTimestamp("a",1000L);
                sourceContext.collectWithTimestamp("a",2000L);
                //sourceContext.emitWatermark(new Watermark(0L));
                sourceContext.collectWithTimestamp("a",1000L);
            }

            @Override
            public void cancel() {

            }
        })
                .keyBy(r -> true)
                .process(new KeyedProcessFunction<Boolean, String, String>() {
                    @Override
                    public void processElement(String s, Context context, Collector<String> collector) throws Exception {
                        if(context.timestamp() < context.timerService().currentWatermark()){
                            collector.collect("迟到数据来了，" + s + "," +context.timestamp());
                        }else {
                            collector.collect("数据没有迟到，" +s + "," +context.timestamp());
                        }
                    }
                })
                .print();


        env.execute();
    }
}
