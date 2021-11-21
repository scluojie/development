package com.flink.day02;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

//reduce的使用
public class Example5 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        env.setParallelism(1);


        DataStreamSource<Integer> stream = env.addSource(new SourceFunction<Integer>() {
            private boolean running = true;
            private Random random = new Random();

            @Override
            public void run(SourceContext<Integer> sourceContext) throws Exception {
                while (running) {
                    sourceContext.collect(random.nextInt(1000));
                    Thread.sleep(100L);
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        });

        //求和
        stream.keyBy(r -> true)
                .reduce(new ReduceFunction<Integer>() {
                    @Override
                    public Integer reduce(Integer value1, Integer value2) throws Exception {
                        return value1 + value2;
                    }
                })
                .print();

        //求最大值
        stream.keyBy(r -> true)
                .reduce(new ReduceFunction<Integer>() {
                    @Override
                    public Integer reduce(Integer value1, Integer value2) throws Exception {
                        return value1 > value2 ? value1 : value2;
                    }
                })
                .print();

        //求平均值
        stream.map(r -> Tuple2.of(r,1))
                .returns(Types.TUPLE(Types.INT,Types.INT))
                .keyBy(r -> true)
                .reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> tuple1, Tuple2<Integer, Integer> tuple2) throws Exception {
                        return Tuple2.of(tuple1.f0 + tuple2.f0,tuple1.f1 + tuple2.f1);
                    }
                })
                .map(new MapFunction<Tuple2<Integer, Integer>, Double>() {
                    @Override
                    public Double map(Tuple2<Integer, Integer> tuple) throws Exception {
                        return Double.valueOf(tuple.f0 / tuple.f1);
                    }
                })
                .print();
        //求最大值最小值
        stream.map(r -> Tuple2.of(r,r))
                .returns(Types.TUPLE(Types.INT,Types.INT))
                .keyBy(r -> true)
                .reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> tuple1, Tuple2<Integer, Integer> tuple2) throws Exception {
                        return Tuple2.of(
                                tuple1.f0 > tuple2.f0 ? tuple1.f0 : tuple2.f0,
                                tuple1.f0 < tuple2.f0 ? tuple1.f0 : tuple2.f0

                        );
                    }
                })
                .print();

        env.execute();
    }
}
