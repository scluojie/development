package com.flink.day01;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Example5 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env .fromElements(1,2,3,4)
                .map(num-> Tuple2.of("key",num))
                .returns(Types.TUPLE(Types.STRING,Types.INT))
                .keyBy(num ->num.f0)
                .sum(1)
                .print();

        env.execute();
    }
}
