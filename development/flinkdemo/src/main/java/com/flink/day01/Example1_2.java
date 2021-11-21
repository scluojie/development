package com.flink.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

//word count
//数据来自socKet
public class Example1_2 {
    public static void main(String[] args) throws Exception {
        //获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行任务的数量为1
        env.setParallelism(1);

        SingleOutputStreamOperator<WordWithCount> mapprdStream = env
                .socketTextStream("hadooop", 9999)
                .flatMap(new FlatMapFunction<String, WordWithCount>() {
                    @Override
                    public void flatMap(String s, Collector<WordWithCount> collector) throws Exception {
                        String[] arr = s.split(" ");
                        for (String word : arr) {
                            collector.collect(new WordWithCount(word, 1L));
                        }
                    }
                });

        //shuffle操作
        KeyedStream<WordWithCount, String> keyedStream = mapprdStream.keyBy(new KeySelector<WordWithCount, String>() {
            @Override
            public String getKey(WordWithCount wordWithCount) throws Exception {
                return wordWithCount.word;
            }
        });

        //reduce操作
        SingleOutputStreamOperator<WordWithCount> result = keyedStream.reduce(new ReduceFunction<WordWithCount>() {
            @Override
            public WordWithCount reduce(WordWithCount wordWithCount, WordWithCount t1) throws Exception {
                //定义的是输入元素和累加器的聚合逻辑
                return new WordWithCount(wordWithCount.word, wordWithCount.count + t1.count);
            }
        });

        result.print();
        //执行
        env.execute();
    }

    //定义一个静态内部类
    //类必须是公共的
    //所有字段必须是公共的
    //必须有空构造器
    //等价于scala中的样例类
    public static class WordWithCount{
        public String word;
        public Long count;

        public WordWithCount() {
        }

        public WordWithCount(String word, Long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordWithCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
