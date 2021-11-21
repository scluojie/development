package com.flink.day09;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class Example4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Tuple2<String, String>> stream = env
                .fromElements(
                        Tuple2.of("Mary", "./home"),
                        Tuple2.of("Bob", "./cart"),
                        Tuple2.of("Mary", "./prod?id=1"),
                        Tuple2.of("Liz", "./home")
                );
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env, settings);

        Table table = streamTableEnvironment.fromDataStream(
                stream,
                $("f0").as("user"),
                $("f1").as("url")
        );
        //要将动态表注册为临时视图 以便使用sql语法进行查询
        streamTableEnvironment.createTemporaryView("clicks",table);

        //在临时视图上进行查询
        Table result = streamTableEnvironment.sqlQuery("select user,count(url) as cnt from clicks group by user");

        //由于查询中存在聚合操作，所以使用toChangeLogStream方法
        DataStream<Row> rowDataStream = streamTableEnvironment.toChangelogStream(result);

        //-U表示撤回数据 +U表示更新数据
        //将旧的聚合结果撤回 发送新的聚合结果
        rowDataStream.print();
        env.execute();
    }
}
