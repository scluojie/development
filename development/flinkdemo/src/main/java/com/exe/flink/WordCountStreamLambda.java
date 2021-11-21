package com.exe.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 有界流 wordcount 文件
 */
public class WordCountStreamLambda {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.读取数据
        DataStreamSource<String> inputDS = env.readTextFile("input/word.txt");


        //3.处理数据
        //3.1压平 切分 转换二元组（word,1）
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOneDS = inputDS.flatMap((FlatMapFunction<String, Tuple2<String, Long>>) (s, collector) -> {
            String[] arr = s.split(" ");
            for (String word : arr) {
                collector.collect(Tuple2.of(word, 1L));
            }
        });
        wordAndOneDS.returns(Types.TUPLE(Types.STRING,Types.LONG));
        KeyedStream<Tuple2<String, Long>, Tuple> wordAndOneKS = wordAndOneDS.keyBy(0);

        SingleOutputStreamOperator<Tuple2<String, Long>> resultDS = wordAndOneKS.sum(1);


        //4.输出
        resultDS.print();

        //5.启动
        env.execute();
    }
}
