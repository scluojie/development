package com.demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author kevin
 * @date 2021/10/28
 * @desc
 */
public class Example02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        env
                .fromElements("hello world","hello flink")
                .flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] arr = value.split(" ");
                        for (String s : arr) {
                            //使用collect方法向下游发送数据
                            out.collect(new Tuple2<String,Integer>(s,1));
                        }
                    }
                })
                .keyBy(tuple -> tuple.f0)
                .reduce((tuple1,tuple2) -> new Tuple2<String,Integer>(tuple1.f0,tuple1.f1 + tuple2.f1))
                .print();

        env.execute();
    }
}
