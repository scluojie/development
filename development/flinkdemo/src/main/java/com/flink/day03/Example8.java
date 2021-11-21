package com.flink.day03;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Calendar;
import java.util.Random;

//连续1秒钟温度上升
public class Example8 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new SensorSource())
                .keyBy(r -> r.id)
                .process(new KeyedProcessFunction<String, SensorReading, String>() {
                    //用来保存最近一次温度
                    private ValueState<Double> lastTemp;
                    //保存定时器时间戳的状态数量
                    private ValueState<Long> timerTs;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        lastTemp = getRuntimeContext().getState(
                                new ValueStateDescriptor<Double>("last-temp", Types.DOUBLE)
                        );
                        timerTs = getRuntimeContext().getState(
                                new ValueStateDescriptor<Long>("timer", Types.LONG)
                        );
                    }

                    @Override
                    public void processElement(SensorReading sensorReading, Context context, Collector<String> collector) throws Exception {
                        Double prevTemp = null;
                        //如果不是第一条数据 将最近一次温度取出来
                        if(lastTemp.value() != null){
                            prevTemp = lastTemp.value();
                        }
                        lastTemp.update(sensorReading.temperature);

                        Long ts = null;
                        //如果有报警定时器存在 将时间戳取出
                        if(timerTs.value() != null){
                             ts = timerTs.value();
                        }

                        //1 2 3 4 5 6
                        if ( prevTemp == null || sensorReading.temperature < prevTemp){
                            if(ts != null)
                                context.timerService().deleteProcessingTimeTimer(ts);

                        }else if(sensorReading.temperature > prevTemp && ts ==null){
                            context.timerService().registerProcessingTimeTimer(
                                    context.timerService().currentProcessingTime() + 1000L
                            ) ;
                            timerTs.update(context.timerService().currentProcessingTime() + 1000L);
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        //定时器用来报警
                        out.collect("温度连续1秒钟上升了,传感器id是：" + ctx.getCurrentKey());
                        timerTs.clear();
                    }
                })
                .print();
        env.execute();
    }

    public static class SensorSource implements SourceFunction<SensorReading> {
        private boolean running = true;
        private Random random = new Random();
        private String[] sensorIds = {
                "sensor_1",
                "sensor_2",
                "sensor_3",
                "sensor_4",
                "sensor_5",
                "sensor_6",
                "sensor_7",
                "sensor_8",
                "sensor_9",
                "sensor_10",

        };

        @Override
        public void run(SourceContext<SensorReading> sourceContext) throws Exception {
            double[] temps = new double[10];
            for (int i = 0; i < 10; i++) {
                temps[i] = 65 + 20 * random.nextGaussian();
            }
            while(running){
                long currTs = Calendar.getInstance().getTimeInMillis();
                for (int i = 0; i < 10; i++) {
                    sourceContext.collect(SensorReading.of(
                            sensorIds[i],
                            temps[i] + random.nextGaussian() * 0.5,
                            currTs
                    ));
                }
                Thread.sleep(300L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    public static class SensorReading{
        public String id;
        private Double temperature;
        private Long timestamp;

        public SensorReading() {
        }

        public SensorReading(String id, Double temperature, Long timestamp) {
            this.id = id;
            this.temperature = temperature;
            this.timestamp = timestamp;
        }

        public static SensorReading of(String id ,Double temperature,Long timestamp)
        {
            return  new SensorReading(id,temperature,timestamp);
        }

        @Override
        public String toString() {
            return "SensorReading{" +
                    "id='" + id + '\'' +
                    ", temperature=" + temperature +
                    ", timestamp=" + timestamp +
                    '}';
        }
    }
}
