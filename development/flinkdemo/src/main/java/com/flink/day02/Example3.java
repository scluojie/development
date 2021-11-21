package com.flink.day02;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

//filter的使用
public class Example3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.addSource(new Example2.CustomSource())
                .filter(new FilterFunction<Example2.Event>() {
                    @Override
                    public boolean filter(Example2.Event event) throws Exception {
                        return event.user.equals("Bob");
                    }
                })
                .print();

        env.addSource(new Example2.CustomSource())
                .filter(event -> event.user.equals("Bob"))
                .print();

        env.addSource(new Example2.CustomSource())
                .filter(new MyFilter())
                .print();
        env.execute();
    }

    public static class MyFilter implements FilterFunction<Example2.Event>{

        @Override
        public boolean filter(Example2.Event event) throws Exception {
            return event.user.equals("Bob");
        }
    }
}
