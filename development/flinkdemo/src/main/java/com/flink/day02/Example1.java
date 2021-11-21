package com.flink.day02;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.awt.*;
import java.util.Calendar;
import java.util.Random;

//自定义点击流数据源
public class Example1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.addSource(new CustomSource()).print();

        env.execute();
    }

    public static class CustomSource implements SourceFunction<Event>{

        private boolean running = true;
        private Random rabdom = new Random();
        private String[] userArr = {"Mary","Bob","Alice","Liz"};
        private String[] urlArr = {"./home","./cart","./fav","./prod?id=1","./prod?id=2"};

        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            while (running){
                //使用collect方法输出数据
                ctx.collect(
                        new Event(
                                userArr[rabdom.nextInt(userArr.length)],
                                urlArr[rabdom.nextInt(urlArr.length)],
                                Calendar.getInstance().getTimeInMillis()
                )
                );
                Thread.sleep(100L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    public static class Event {
        public String user;
        public String url;
        public Long timestamp;

        public Event() {
        }

        public Event(String user, String url, Long timestamp) {
            this.user = user;
            this.url = url;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return "Event{" +
                    "user='" + user + '\'' +
                    ", url='" + url + '\'' +
                    ", timestamp=" + timestamp +
                    '}';
        }
    }
}
