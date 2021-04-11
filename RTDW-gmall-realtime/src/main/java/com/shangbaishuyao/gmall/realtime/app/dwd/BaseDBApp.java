package com.shangbaishuyao.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.shangbaishuyao.gmall.realtime.app.func.DimSink;
import com.shangbaishuyao.gmall.realtime.app.func.TableProcessFunction;
import com.shangbaishuyao.gmall.realtime.bean.TableProcess;
import com.shangbaishuyao.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * Desc: 准备业务数据的DWD层 <br/>
 * create by shangbaishuyao on 2021/4/9
 * @Author: 上白书妖
 * @Date: 15:48 2021/4/9
 */
public class BaseDBApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.准备环境
        //1.1 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并新度
        env.setParallelism(1);
        //1.3 开启Checkpoint，并设置相关的参数
        //env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointTimeout(60000);
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop202:8020/gmall/checkpoint/basedbapp"));
        //重启策略
        // 如果说没有开启重启Checkpoint，那么重启策略就是noRestart
        // 如果说没有开Checkpoint，那么重启策略会尝试自动帮你进行重启   重启Integer.MaxValue
        //env.setRestartStrategy(RestartStrategies.noRestart());

        //TODO 2.从Kafka的ODS层读取数据
        String topic = "ods_base_db_m";
        String groupId = "base_db_app_group";

        //2.1 通过工具类获取Kafka的消费者
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        DataStreamSource<String> jsonStrDS = env.addSource(kafkaSource);

        //TODO 3.对DS中数据进行结构的转换      String-->Json
        //jsonStrDS.map(JSON::parseObject);
        SingleOutputStreamOperator<JSONObject> jsonObjDS = jsonStrDS.map(jsonStr -> JSON.parseObject(jsonStr));
        //jsonStrDS.print("json>>>>");

        //TODO 4.对数据进行ETL   如果table为空 或者 data为空，或者长度<3  ，将这样的数据过滤掉
        SingleOutputStreamOperator<JSONObject> filteredDS = jsonObjDS.filter(
            jsonObj -> {
                boolean flag = jsonObj.getString("table") != null && jsonObj.getJSONObject("data") != null && jsonObj.getString("data").length() > 3;
                return flag;
            }
        );

        //filteredDS.print("json>>>>>");

        //TODO 5. 动态分流  事实表放到主流，写回到kafka的DWD层；如果维度表，通过侧输出流，写入到Hbase
        //5.1定义输出到Hbase的侧输出流标签
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>(TableProcess.SINK_TYPE_HBASE){};

        //5.2 主流 写回到Kafka的数据
        SingleOutputStreamOperator<JSONObject> kafkaDS = filteredDS.process(
            new TableProcessFunction(hbaseTag)
        );

        //5.3获取侧输出流    写到Hbase的数据
        DataStream<JSONObject> hbaseDS = kafkaDS.getSideOutput(hbaseTag);

        kafkaDS.print("事实>>>>");
        hbaseDS.print("维度>>>>");

        //TODO 6.将维度数据保存到Phoenix对应的维度表中
        hbaseDS.addSink(new DimSink());

        //TODO 7.将事实数据写回到kafka的dwd层
        FlinkKafkaProducer<JSONObject> kafkaSink = MyKafkaUtil.getKafkaSinkBySchema(
            new KafkaSerializationSchema<JSONObject>() {
                @Override
                public void open(SerializationSchema.InitializationContext context) throws Exception {
                    System.out.println("kafka序列化");
                }
                @Override
                public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObj, @Nullable Long timestamp) {
                    String sinkTopic = jsonObj.getString("sink_table");
                    JSONObject dataJsonObj = jsonObj.getJSONObject("data");
                    return new ProducerRecord<>(sinkTopic,dataJsonObj.toString().getBytes());
                }
            }
        );

        kafkaDS.addSink(kafkaSink);

        env.execute();
    }
}
