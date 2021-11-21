package com.flink.day02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.time.LocalDate;
import java.util.Calendar;
import java.util.Random;

//map的使用
public class Example2 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.addSource(new CustomSource())
                .map(new MapFunction<Event, String>() {

                    @Override
                    public String map(Event event) throws Exception {
                        return event.user;
                    }
                })
                .print();

        env.addSource(new CustomSource())
                .map(event->event.user)
                .print();

        env.addSource(new CustomSource())
                .flatMap(new FlatMapFunction<Event, String>() {
                    @Override
                    public void flatMap(Event event, Collector<String> collector) throws Exception {
                        collector.collect(event.user);
                    }
                })
                .print();

        env.execute();


    }

    public static class CustomSource implements SourceFunction<Event>{
        private boolean running =true;
        private Random random = new Random();
        private String[] userArr = new String[]{"Bob","Alice","kous","beata"};
        private String[] urlArr = {"./home","./fav","./payment","prod?id=1","prod?id=2"};
        @Override
        public void run(SourceContext<Event> sourceContext) throws Exception {
            while(running){
                sourceContext.collect(new Event(
                        userArr[random.nextInt(userArr.length)],
                        urlArr[random.nextInt(urlArr.length)],
                        Calendar.getInstance().getTimeInMillis()
                ));
                Thread.sleep(100L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    public static class Event{
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
