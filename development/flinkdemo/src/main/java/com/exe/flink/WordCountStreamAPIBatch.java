package com.exe.flink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 流式API下的批处理 =》体现流批一体
 */
public class WordCountStreamAPIBatch {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //TODO 流的执行环境下 实现 批处理
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        //2.读取数据
        DataStreamSource<String> inputDS = env.readTextFile("input/word.txt");


        //3.处理数据
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOneDS = inputDS.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {

            @Override
            public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                String[] arr = s.split(" ");
                //使用采集器输出
                for (String word : arr) {

                    collector.collect(Tuple2.of(word, 1L));
                }
            }
        });

        KeyedStream<Tuple2<String, Long>, Tuple> keyByDS = wordAndOneDS.keyBy(0);

        SingleOutputStreamOperator<Tuple2<String, Long>> result = keyByDS.sum(1);

        //4.输出数据
        result.print();

        env.execute();
    }
}
