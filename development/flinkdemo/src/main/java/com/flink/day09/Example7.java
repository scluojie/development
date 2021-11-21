package com.flink.day09;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

//DDL
public class Example7 {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env, settings);

        /*streamTableEnvironment.executeSql(
                "create table clicks (`user` String,`url` String)" +
                        "with (" +
                        "'connector' = 'filesystem'," +
                        "'path' = 'D:\\development\\flinkdemo\\src\\main\\resources\\UserBehavior.csv'," +
                        "'format' = 'csv'" +
                        ")"

        );*/
        streamTableEnvironment
                 .executeSql(
                        "CREATE TABLE clicks (`user` String, `url` String) " +
                                "WITH (" +
                                "'connector' = 'filesystem'," +
                                "'path' = 'D:\\development\\flinkdemo\\src\\main\\resources\\file.csv'," +
                                "'format' = 'csv'" +
                                ")"
                );

        streamTableEnvironment.executeSql("create table ResultTable (`user` STRING,`cnt` BIGINT) " +
                "with ('connector' = 'print')");

        streamTableEnvironment.executeSql("insert into ResultTable select user,count(url) as cnt from clicks group by user ");


    }
}
