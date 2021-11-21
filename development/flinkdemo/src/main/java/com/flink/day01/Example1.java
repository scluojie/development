package com.flink.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

//word count
//数据来自socket
public class Example1 {
    public static void main(String[] args) throws Exception {
        //获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行任务的数量为1
        env.setParallelism(1);

        //从socket读取数据源
        //先启动nc -lk 9999
        DataStreamSource<String> stream = env.socketTextStream("hadoop102", 8888);

        //map 操作 s=>(s,1)
        SingleOutputStreamOperator<WordWithCount> mappedStream = stream.flatMap(new FlatMapFunction<String, WordWithCount>() {
            @Override
            public void flatMap(String line, Collector<WordWithCount> out) throws Exception {
                String[] arr = line.split(" ");
                //向下游发送数据
                for (String word : arr) {
                    //使用collect 方法向下游发送数据
                    out.collect(new WordWithCount(word, 1L));
                }
            }
        });
        //shuffle操作
        KeyedStream<WordWithCount, String> keyedStream = mappedStream.keyBy(new KeySelector<WordWithCount, String>() {
            @Override
            public String getKey(WordWithCount wordWithCount) throws Exception {
                //为数据指定key
                return wordWithCount.word;
            }
        });
        //reduce操作
        SingleOutputStreamOperator<WordWithCount> result = keyedStream.reduce(new ReduceFunction<WordWithCount>() {
            @Override
            public WordWithCount reduce(WordWithCount wordWithCount1, WordWithCount wordWithCount2) throws Exception {
                //定义的是输入元素和累加器的聚合逻辑
                return new WordWithCount(wordWithCount1.word, wordWithCount1.count + wordWithCount2.count);
            }
        });
        result.print();
        //执行程序
        env.execute(); //有异常
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
