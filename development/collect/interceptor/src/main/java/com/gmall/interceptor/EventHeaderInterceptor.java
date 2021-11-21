package com.gmall.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class EventHeaderInterceptor implements Interceptor {

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        Map<String, String> headers = event.getHeaders();
        String body = new String(event.getBody(), StandardCharsets.UTF_8);
        if(body.contains("kous")){
            headers.put("topic","kous");
        }else if(body.contains("kevin")){
            headers.put("topic","kevin");
        }else{
            headers.put("topic","other");
        }
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        for (Event event : events) {
            intercept(event);
        }
        return events;
    }

    @Override
    public void close() {

    }

    public static class MyBuilder implements Builder{

        @Override
        public Interceptor build() {
            return new EventHeaderInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
