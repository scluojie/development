package com.demo;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author kevin
 * @date 2021/10/28
 * @desc
 */
public class Example01 {
    public static void main(String[] args) throws Exception {
        //获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> DS = env.fromElements(1, 2, 3, 4, 5, 6, 8, 12);


        DS
                .map(num -> Tuple2.of("key",num))
                .returns(Types.TUPLE(Types.STRING,Types.INT))
                .keyBy(tuple ->tuple.f0)
                .sum(1)
                .print();
        env.execute();

    }

}
