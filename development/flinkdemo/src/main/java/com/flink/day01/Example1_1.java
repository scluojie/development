package com.flink.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

//从socket 读取数据 实现wordcount
public class Example1_1 {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(1);

        //获取数据 从socket
        env
                //.socketTextStream("hadoop102",9999)
                .fromElements("hello word","hello flink")
                .flatMap(new FlatMapFunction<String, Tuple2<String,Long>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                        //每一行数据进行分割
                        String[] arr = s.split(" ");
                        //使用collector向下游发送数据
                       Arrays.stream(arr).forEach(word ->  collector.collect(Tuple2.of(word,1L)));
                    }
                })
                //.returns(Types.TUPLE(Types.STRING,Types.LONG))
                .keyBy(new KeySelector<Tuple2<String, Long>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Long> tuple) throws Exception {
                        return tuple.f0;
                    }
                })
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> tuple1, Tuple2<String, Long> tuple2) throws Exception {
                        return Tuple2.of(tuple1.f0,tuple1.f1 + tuple2.f1);
                    }
                })
                .print();



        //执行
        env.execute();
    }
}
