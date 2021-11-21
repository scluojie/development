package com.flink.day02;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Example6 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.fromElements(1,2,3,4)
                .shuffle()
                .print()
                .setParallelism(2);

        env.execute();
    }
}
