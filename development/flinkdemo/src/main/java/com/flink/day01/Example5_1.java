package com.flink.day01;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Example5_1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .fromElements(1,2,3,4)
                .map(num-> Tuple2.of("key",num))
                .returns(Types.TUPLE(Types.STRING,Types.INT))
                .keyBy(tuple->tuple.f0)
                .sum(1)
                .print();
        env.execute();
    }
}
