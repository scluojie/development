package com.flink.day01;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


//lambda 实现word count
public class Example3_1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .fromElements("hello spark","hello flink")
                .flatMap((String line, Collector<Tuple2<String,Long>> out) -> {
                    String[] arr = line.split(" ");
                    for (String word : arr) {
                        out.collect(Tuple2.of(word,1L));
                    }
                })
                .returns(Types.TUPLE(Types.STRING,Types.LONG))
                .keyBy(tuple -> tuple.f0)
                .reduce((Tuple2<String,Long> tuple1,Tuple2<String,Long> tuple2)->
                    Tuple2.of(tuple1.f0,tuple1.f1 + tuple2.f1)
                )
                .returns(Types.TUPLE(Types.STRING,Types.LONG))
                .print();
        env.execute();
    }
}
