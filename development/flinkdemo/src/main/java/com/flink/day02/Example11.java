package com.flink.day02;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class Example11 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.fromElements(1,2,3)
                .addSink(new RichSinkFunction<Integer>() {
                    @Override
                    public void invoke(Integer value, Context context) throws Exception {
                        super.invoke(value, context);
                        System.out.println(value);
                    }
                });
        env.execute();
    }
}
