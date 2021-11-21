package com.flink.day05;

import com.flink.day03.Example8;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

//状态后端
public class Example2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        //每隔10秒钟，做一次检查点保存操作
        env.enableCheckpointing(10*1000L);
        //设置状态后端
        env.setStateBackend(new FsStateBackend("file:///D:\\development\\flinkdemo\\src\\main\\resources\\ckpt",false));
        env
                .addSource(new Example8.SensorSource())
                .print();
        env.execute();
    }

}
