package com.shangbaishuyao.connectors.es7;

import com.shangbaishuyao.common.constant.PropertiesConstants;
import com.shangbaishuyao.common.model.MetricEvent;
import com.shangbaishuyao.common.utils.ExecutionEnvUtil;
import com.shangbaishuyao.common.utils.GsonUtil;
import com.shangbaishuyao.common.utils.KafkaConfigUtil;
import com.shangbaishuyao.connectors.es7.utils.ESSinkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.List;

/**
 * Desc:  sink data to es7
 * create by shangbaishuyao on 2021/2/1
 * @Author: 上白书妖
 * @Date: 17:20 2021/2/1
 */
@Slf4j
public class Sink2ES7Main {
    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        DataStreamSource<MetricEvent> data = KafkaConfigUtil.buildSource(env);

        List<HttpHost> esAddresses = ESSinkUtil.getEsAddresses(parameterTool.get(PropertiesConstants.ELASTICSEARCH_HOSTS));
        int bulkSize = parameterTool.getInt(PropertiesConstants.ELASTICSEARCH_BULK_FLUSH_MAX_ACTIONS, 40);
        int sinkParallelism = parameterTool.getInt(PropertiesConstants.STREAM_SINK_PARALLELISM, 5);

        log.info("-----esAddresses = {}, parameterTool = {}, ", esAddresses, parameterTool);

        ESSinkUtil.addSink(esAddresses, bulkSize, sinkParallelism, data,
                (MetricEvent metric, RuntimeContext runtimeContext, RequestIndexer requestIndexer) -> {
                    requestIndexer.add(Requests.indexRequest()
                            .index(PropertiesConstants.SHANGBAISHUYAO + "_" + metric.getName())
                            .type(PropertiesConstants.SHANGBAISHUYAO)
                            .source(GsonUtil.toJSONBytes(metric), XContentType.JSON));
                },
                parameterTool);
        env.execute("flink learning connectors es6");
    }
}
