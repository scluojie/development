package com.flume;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

import java.util.HashMap;
import java.util.Map;

public class MySource extends AbstractSource implements Configurable, PollableSource {

    //定义将来要读取的字段
    private Long delay;
    private String field;

    @Override
    public Status process() throws EventDeliveryException {
        try {
            //创建事件头信息
            Map<String, String> headerMap = new HashMap<>();
            //创建事件
            SimpleEvent event = new SimpleEvent();
            //循环封装事件
            for (int i = 0; i < 5; i++) {
                //给事件设置头信息
                event.setHeaders(headerMap);
                //给事件设置内容
                event.setBody((field + i).getBytes());
                //将实践写入channel
                getChannelProcessor().processEvent(event);

                Thread.sleep(delay);

            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            return Status.BACKOFF;
        }
        return Status.READY;
    }

    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }

    @Override
    public void configure(Context context) {
        delay = context.getLong("delay");
        field = context.getString("field", "Hello!");
    }
}
