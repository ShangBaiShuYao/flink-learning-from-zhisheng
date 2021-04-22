package com.shangbaishuyao.demo.Function;

import com.shangbaishuyao.demo.bean.WaterSensor;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.HashMap;

public class MyEsSinkFunc implements ElasticsearchSinkFunction<WaterSensor> {
    @Override
    public void process(WaterSensor element, RuntimeContext ctx, RequestIndexer indexer) {
        HashMap<String, String> source = new HashMap<>();
        source.put("ts", element.getTs().toString());
        source.put("vc", element.getVc().toString());
        //创建Index请求
        IndexRequest indexRequest = Requests.indexRequest()
                .index("sensor1")
                .type("_doc")
//                    .id(element.getId())
                .source(source);
        //写入ES
        indexer.add(indexRequest);
    }
}
