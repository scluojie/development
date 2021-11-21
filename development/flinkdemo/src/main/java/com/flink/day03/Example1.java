package com.flink.day03;

import com.flink.day02.Example2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

//全窗口聚合函数的使用
//ProcessWindowFunction
public class Example1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();;
        env.setParallelism(1);

        DataStreamSource<Example2.Event> stream = env.addSource(new Example2.CustomSource());

        KeyedStream<Example2.Event, String> keyedStream = stream.keyBy(r -> r.user);

        WindowedStream<Example2.Event, String, TimeWindow> windowedStream = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5)));

        windowedStream.process(new WindowResult()).print();
        env.execute();


    }

    public static class WindowResult extends ProcessWindowFunction<Example2.Event,UserViewCountPerWindow,String,TimeWindow> {

        @Override
        public void process(String key, Context context, Iterable<Example2.Event> elements, Collector<UserViewCountPerWindow> collector) throws Exception {
            //迭代器中是当前窗口的所有元素
            //窗口关闭的时候，触发process方法的执行
            long windowStart = context.window().getStart();
            long windowEnd = context.window().getEnd();
            long count = elements.spliterator().getExactSizeIfKnown();//获取迭代器中元素个数
            collector.collect(new UserViewCountPerWindow(key,count,windowStart,windowEnd));
        }
    }

    public static class UserViewCountPerWindow{
        public String user;
        public Long count;
        public Long windowStart;
        public Long windowEnd;

        public UserViewCountPerWindow() {
        }

        public UserViewCountPerWindow(String user, Long count, Long windowStart, Long windowEnd) {
            this.user = user;
            this.count = count;
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
        }

        @Override
        public String toString() {
            return "UserViewCountPerWindow{" +
                    "user='" + user + '\'' +
                    ", count=" + count +
                    ", windowStart=" + windowStart +
                    ", windowEnd=" + windowEnd +
                    '}';
        }
    }
}
