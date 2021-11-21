package com.flink.day07;

import com.flink.day02.Example2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * 写入redis
 */
public class Example3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("hadoop102").build();

        env
                .addSource(new Example2.CustomSource())
                .addSink(new RedisSink<Example2.Event>(conf,new MyRedisMapper()));
        env.execute();
    }

    public static class MyRedisMapper implements RedisMapper<Example2.Event> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            //hset clicks user url
            return new RedisCommandDescription(RedisCommand.HSET,"clicks");
        }

        @Override
        public String getKeyFromData(Example2.Event event) {
            return event.user;
        }

        @Override
        public String getValueFromData(Example2.Event event) {
            return event.url;
        }
    }
}
