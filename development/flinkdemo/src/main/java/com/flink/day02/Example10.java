package com.flink.day02;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class Example10 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new RichParallelSourceFunction<String>() {
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        System.out.println("子任务索引为：" + getRuntimeContext().getTaskNameWithSubtasks() + "的生命周期开始");
                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                        System.out.println("子任务索引为：" + getRuntimeContext().getTaskNameWithSubtasks());
                    }

                    @Override
                    public void run(SourceContext<String> sourceContext) throws Exception {
                        for (int i = 0; i < 10 ; i++) {

                            if(i % 2 == getRuntimeContext().getIndexOfThisSubtask()){
                                sourceContext.collect("并行子任务索引为：" + getRuntimeContext().getTaskNameWithSubtasks() + "发送数据" + i);
                            }else{
                                sourceContext.collect("并行子任务索引为：" + getRuntimeContext().getTaskNameWithSubtasks() + "没有发送数据" + i);
                            }
                        }
                    }

                    @Override
                    public void cancel() {

                    }
                })
                .setParallelism(2)
                .print()
                .setParallelism(2);
        env.execute();
    }
}
