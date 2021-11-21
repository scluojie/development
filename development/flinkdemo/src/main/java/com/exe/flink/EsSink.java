package com.exe.flink;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @author kevin
 * @date 2021/9/19
 * @desc
 */
public class EsSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> inputDS = env.socketTextStream("hadoop102", 9999);

        //TODO Sink -Elasticsearch
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("hadoop102",9200));
        ElasticsearchSink.Builder<String> esBuilder = new ElasticsearchSink.Builder<>(httpHosts,
                new ElasticsearchSinkFunction<String>() {
                    @Override
                    public void process(String element, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                        //ESAPI的写法
                        HashMap<String, String> hashMap = new HashMap<>();
                        hashMap.put("data", element);
                        IndexRequest indexRequest = Requests.indexRequest("es").type("tab").source(hashMap);
                        requestIndexer.add(indexRequest);
                    }
                });
        //为了演示 设置为1 就刷写
        esBuilder.setBulkFlushMaxActions(1);

        ElasticsearchSink<String> build = esBuilder.build();


        inputDS.addSink(build);

        env.execute();
    }
}
