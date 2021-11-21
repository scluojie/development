package com.flink.day02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

//flatMap的使用
//针对流中的一个元素生成0个、1个或者多个元素
//flatMap是map和filter的泛化
public class Example4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> stream = env.fromElements("white", "balck", "gray");

        stream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> collector) throws Exception {
                if(value.equals("white")){
                    collector.collect(value);
                }else if (value.equals("black")){
                    collector.collect(value);
                    collector.collect(value);
                }
            }
        })
                .print();

        stream.flatMap((String value,Collector<String> out)->{
            if(value.equals("white")){
                out.collect(value);
            }else if (value.equals("black")){
                out.collect(value);
                out.collect(value);
            }
        })
                .returns(Types.STRING)
                        .print();
        env.execute();
    }
}
