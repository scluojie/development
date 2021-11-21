package com.flink.day05;

import com.flink.day02.Example2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

//查询流
public class Example3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Example2.Event> clickStream = env.addSource(new Example2.CustomSource());

        DataStreamSource<String> queryStream = env.socketTextStream("hadoop102", 9999);
        
        clickStream.keyBy(r -> r.user)
                .connect(queryStream.setParallelism(1).broadcast())
                .flatMap(new CoFlatMapFunction<Example2.Event, String, Example2.Event>() {
                    private String query = "";
                    @Override
                    public void flatMap1(Example2.Event value, Collector<Example2.Event> out) throws Exception {
                        //用来处理来自第一条流的数据
                        if(value.url.equals(query))
                            out.collect(value);
                    }

                    @Override
                    public void flatMap2(String value, Collector<Example2.Event> out) throws Exception {
                        //处理来自第二条流的数据
                        query = value;
                    }
                })
                .print();
        env.execute();
    }
}
