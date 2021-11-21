package com.exe.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author kevin
 * @date 2021/9/21
 * @desc flinkSql 从文件读取数据
 */
public class TableAPI_FileSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO -connector外部系统 ，读文件
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv
                .connect(new FileSystem().path("input/sensor.csv"))
                .withFormat(new Csv().fieldDelimiter(','))
                .withSchema(new Schema()
                    .field("id", DataTypes.STRING())
                        .field("ts",DataTypes.BIGINT())
                        .field("vc",DataTypes.INT())
                ).createTemporaryTable("fileTable");

        //通过表名获取table对象
        Table fileTable = tableEnv.from("fileTable");

        Table resultTable = fileTable
                .where($("vc").isGreaterOrEqual(3))
                .select($("*"));

        tableEnv.toAppendStream(resultTable, Row.class).print();

        env.execute();
    }
}
