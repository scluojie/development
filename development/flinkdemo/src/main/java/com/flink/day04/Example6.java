package com.flink.day04;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class Example6 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        env
                .addSource(new SourceFunction<Tuple2<String,Long>>() {
                    @Override
                    public void run(SourceContext<Tuple2<String, Long>> sourceContext) throws Exception {
                        sourceContext.collect(Tuple2.of("a",1000L));
                        Thread.sleep(1000L);
                        sourceContext.collect(Tuple2.of("a",2000L));
                        Thread.sleep(1000L);
                        sourceContext.collect(Tuple2.of("a",3000L));
                        Thread.sleep(1000L);
                        sourceContext.collect(Tuple2.of("a",1000L));
                    }

                    @Override
                    public void cancel() {

                    }
                })
                .assignTimestampsAndWatermarks(new CustomWatermarkStrategy())
        .print();
        env.execute();
    }

    public static class CustomWatermarkStrategy implements WatermarkStrategy<Tuple2<String, Long>>{

        @Override
        public WatermarkGenerator<Tuple2<String, Long>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {

            return new WatermarkGenerator<Tuple2<String, Long>>() {
                private long delay = 0L ; //最大延迟时间
                private long maxTs = -Long.MAX_VALUE + delay + 1L;//观察到的最大时间戳
                @Override
                public void onEvent(Tuple2<String, Long> event, long l, WatermarkOutput watermarkOutput) {
                    //每来一条数据， 试图更新一次观察到的最大时间戳
                    maxTs = Math.max(maxTs,event.f1);
                }

                @Override
                public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
                    //周期性的向数据流中插入水位线
                    watermarkOutput.emitWatermark(new Watermark(maxTs -delay -1));
                }
            };
        }

        @Override
        public TimestampAssigner<Tuple2<String, Long>> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                @Override
                public long extractTimestamp(Tuple2<String, Long> element, long l) {
                    return element.f1; //指定时间戳是哪个字段

                }
            };
        }
    }
}
