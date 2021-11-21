package com.gmall.interceptor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class EventHeaderTimeStampInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        String body = new String(event.getBody(), StandardCharsets.UTF_8);
        Map<String, String> headers = event.getHeaders();
        JSONObject jsonObject = JSON.parseObject(body);
        String timeStamp = jsonObject.getString("ts");
        headers.put("timestamp",timeStamp);
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
            return new EventHeaderTimeStampInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }


}
