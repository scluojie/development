package com.demo;

import org.apache.spark.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author kevin
 * @date 2021/10/30
 * @desc word count
 */
public class StreamWordCount {
    public static void main(String[] args) throws InterruptedException {
        //1.初始化Spark配置信息
        SparkConf conf = new SparkConf().set("spark.driver.host", "localhost").setMaster("local[2]").setAppName("NetworkWordCount");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(2));

        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9000);

        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());

        JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));

        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((i1, i2) -> i1 + i2);

        wordCounts.print();

        jssc.start();

        jssc.awaitTermination();

    }
}
