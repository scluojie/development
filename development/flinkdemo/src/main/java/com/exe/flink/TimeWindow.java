package com.exe.flink;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author kevin
 * @date 2021/9/19
 * @desc 滚动时间窗口
 */
public class TimeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> inputDS = env
                .socketTextStream("hadoop102", 9999);

        //分组之前开窗

        env.execute();
    }
}
