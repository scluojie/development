package com.flink.day01;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Example4 {
    public static void main(String[] args) throws Exception {
        //1.获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.设置并行度
        env.setParallelism(1);

        env.fromElements("hello word","hello flink")
                .flatMap((String line, Collector<WordWithCount> out)->{
                    String[] words = line.split(" ");
                    for (String word : words) {
                        out.collect(new WordWithCount(word,1L));
                    }
                }).setParallelism(1)
                .returns(Types.POJO(WordWithCount.class))
                .keyBy((WordWithCount tuple)->tuple.word)
                .reduce((WordWithCount tuple1,WordWithCount tuple2)->new WordWithCount(tuple1.word, tuple1.count + tuple2.count))
                .print().setParallelism(4);

        //3.执行
        env.execute();
    }

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
