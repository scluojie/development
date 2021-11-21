package com.flink.day07;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * 从文件读取数据 写入kafka
 */
public class Example1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","hadoop102:9092,hadoop103:9092,hadoop104:9092");
        env.readTextFile("D:\\development\\flinkdemo\\src\\main\\resources\\UserBehavior.csv")
                .addSink(new FlinkKafkaProducer<String>(
                        "user-behavior-topic",
                        new SimpleStringSchema(),
                        properties
                ));
        env.execute();
    }
}
