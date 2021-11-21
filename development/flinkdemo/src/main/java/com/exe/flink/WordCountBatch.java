package com.exe.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 使用批处理方式
 */
public class WordCountBatch {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //2.读取数据
        DataSource<String> wordDS = env.readTextFile("input/word.txt");


        //3.处理数据
        FlatMapOperator<String, Tuple2<String, Long>> wordAndOneDS = wordDS.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {

            @Override
            public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                String[] arr = s.split(" ");
                //使用采集器输出
                for (String word : arr) {

                    collector.collect(Tuple2.of(word, 1L));
                }
            }
        });

        UnsortedGrouping<Tuple2<String, Long>> groupDS = wordAndOneDS.groupBy(0);

        AggregateOperator<Tuple2<String, Long>> result = groupDS.sum(1);

        //4.输出数据
        result.print();
    }
}
