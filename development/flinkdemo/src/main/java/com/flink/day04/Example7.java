package com.flink.day04;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

//多流合并的算子union
//多条流中的元素类型必须一样
public class Example7 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> stream1 = env.fromElements(1, 2, 3);
        DataStreamSource<Integer> stream2 = env.fromElements(4, 5, 6);
        DataStreamSource<Integer> stream3 = env.fromElements(7, 8, 9);

        stream1.union(stream2,stream3).print();
        env.execute();
    }

}
