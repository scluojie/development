package com.gmall.interceptor;

import com.alibaba.fastjson.JSONException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.mortbay.util.ajax.JSON;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

public class CheckLogInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        String body = new String(event.getBody(), StandardCharsets.UTF_8);
        try {
            JSON.parse(body);
        }catch (JSONException e){
            return null;
        }
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        /*for (Event event : events) {
            Event result = intercept(event);
            if(result == null){
                events.remove(event);
            }
        }*/
        Iterator<Event> iterator = events.iterator();
        while(iterator.hasNext()){
            Event next = iterator.next();
            Event result = intercept(next);
            if(result == null){
                iterator.remove();
            }
        }
        return events;
    }

    @Override
    public void close() {

    }

    public static class MyBuilder implements Builder{
        @Override
        public Interceptor build() {
            return new CheckLogInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
