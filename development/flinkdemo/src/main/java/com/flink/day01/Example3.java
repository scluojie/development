package com.flink.day01;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class Example3 {
    public static void main(String[] args) throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    env.setParallelism(1);

    env.fromElements("hello word","hello flink")
            .flatMap((String line, Collector<WordWithCount> out)->{
                String[] words = line.split(" ");
                for (String word : words) {
                    out.collect(new WordWithCount(word,1L));
                }
            })
            .returns(Types.POJO(WordWithCount.class))
            .keyBy(tuple->tuple.word)
            .reduce((WordWithCount tuple1,WordWithCount tuple2)->new WordWithCount(tuple1.word,tuple1.count + tuple2.count))
            .print();

    env.execute();
    }

    //定义一个静态内部类
    //类必须是公共类
    //所有字段必须是公共的
    //必须有空构造器
    //等价于scala 中的模式匹配
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
