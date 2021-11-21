package com.exe.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author kevin
 * @date 2021/9/19
 * @desc 自定义Sink
 */
public class MySink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> inputDS = env.socketTextStream("hadoop102", 9999);
        //TODO Sink 自定义 MySQL
        inputDS.addSink(new MysqlSink());


        env.execute();
    }

    public static class MysqlSink extends RichSinkFunction<String> {

        private Connection conn;
        private PreparedStatement preparedStatement;
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/demo", "root", "123456");
            String sql = "insert into customer(id,name,age) values(?,?,?)";
            preparedStatement = conn.prepareStatement(sql);
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            super.invoke(value, context);
            String[] arr = value.split(" ");
            preparedStatement.setInt(1,Integer.parseInt(arr[0]));
            preparedStatement.setString(2,arr[1]);
            preparedStatement.setInt(3,Integer.parseInt(arr[2]));
            preparedStatement.execute();

        }

        @Override
        public void close() throws Exception {
            super.close();
            if(preparedStatement != null){
                preparedStatement.close();
                conn.close();
            }

        }
    }
}
