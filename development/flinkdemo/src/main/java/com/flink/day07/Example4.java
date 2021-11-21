package com.flink.day07;

import com.flink.day02.Example2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

//幂等性写入mysql
public class Example4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new Example2.CustomSource())
                .addSink(new MyJDBC());
        env.execute();
    }

    public static class MyJDBC extends RichSinkFunction<Example2.Event>{
        private Connection connection;
        private PreparedStatement updateinsertStmt;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            connection = DriverManager.getConnection(
                    "jdbc:mysql://hadoop102:3306/flinktest?characterEncoding=utf-8&useSSL=false",
                    "root",
                    "123456");
            updateinsertStmt = connection.prepareStatement("insert into clicks (user,url) values(?,?) on duplicate key update url=?");
        }

        @Override
        public void close() throws Exception {
            super.close();
            updateinsertStmt.close();
            connection.close();
        }


        @Override
        public void invoke(Example2.Event value, Context context) throws Exception {
            super.invoke(value, context);
            updateinsertStmt.setString(1,value.user);
            updateinsertStmt.setString(2,value.url);
            updateinsertStmt.setString(3,value.url);
            updateinsertStmt.executeUpdate();
        }
    }
}
