package com.flink.day05;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

//select * from A inner join B where A.id = B.id;
public class Example4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Tuple2<String, Integer>> stream1 = env
                .fromElements(
                        Tuple2.of("a", 1),
                        Tuple2.of("b", 1)
                );

        DataStreamSource<Tuple2<String, Integer>> stream2 = env
                .fromElements(
                        Tuple2.of("a", 2),
                        Tuple2.of("a", 3),
                        Tuple2.of("b", 2)
                );

        stream1.keyBy(r -> r.f0)
                .connect(stream2.keyBy(r -> r.f0))
                .process(new CoProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String>() {
                    private ListState<Tuple2<String, Integer>> list1;
                    private ListState<Tuple2<String, Integer>> list2;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        list1 = getRuntimeContext().getListState(
                                new ListStateDescriptor<Tuple2<String, Integer>>("list1", Types.TUPLE(Types.STRING, Types.INT))
                        );
                        list2 = getRuntimeContext().getListState(
                                new ListStateDescriptor<Tuple2<String, Integer>>("list2", Types.TUPLE(Types.STRING, Types.INT))
                        );
                    }

                    @Override
                    public void processElement1(Tuple2<String, Integer> value, Context ctx, Collector<String> out) throws Exception {
                        list1.add(value);
                        for (Tuple2<String, Integer> t : list2.get()) {
                            out.collect(value + " => " + t);
                        }
                    }

                    @Override
                    public void processElement2(Tuple2<String, Integer> value, Context ctx, Collector<String> out) throws Exception {
                        list2.add(value);
                        for (Tuple2<String, Integer> t : list1.get()) {
                            out.collect(t + " => " + value);
                        }
                    }
                })
                .print();

        env.execute();
    }
}
