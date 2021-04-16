package com.shangbaishuyao.gmall.realtime.app.DWD;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.shangbaishuyao.gmall.realtime.app.Function.PhoenixSinkFunction;
import com.shangbaishuyao.gmall.realtime.app.Function.TableProcessFunction;
import com.shangbaishuyao.gmall.realtime.bean.TableProcess;
import com.shangbaishuyao.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * Desc: 准备业务数据的DWD层,动态分流 <br/>
 * @Author: 上白书妖
 * @Date: 21:39 2021/4/10
 */
public class BaseDB_ODS_SideOutput_DWDorHbase {
    public static void main(String[] args) throws Exception {
        //TODO 准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度.
        env.setParallelism(1);
        //开启checkPoint并设置相关参数.
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        //设置checkPoint(检查点)延迟时间,抄过一分钟则废弃.
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        //将检查点保存到HDFS上
        env.setStateBackend(new FsStateBackend("hdfs://hadoop202:8020/gmall/checkpoint/basedbapp"));
        //重启策略
        //如果说没有开启重启CheckPoint,那么重启策略就是noRestart(不重启).就是说我检查点关掉了,程序出现问题了,那他就直接抛异常
        //如果说开启了CheckPoint,那么重启策略会尝试自动帮你进行重启,重启Integer.MaxValue这么多次.直接冲正确的地方开始重新执行,知道异常出处理成功了.
        //当然你也可以开启检查点(CheckPoint),但是选着重启策略是不重启.程序有异常直接抛异常报错.
        env.setRestartStrategy(RestartStrategies.noRestart());

        //TODO 2.从Kafka的ODS层读取数据
        String topic = "ods_base_db_m";
        String groupId = "base_db_app_group";
        //2.1 通过工具类获取Kafka的消费者
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        DataStreamSource<String> kafkasourceDataStream = env.addSource(kafkaSource);

        //TODO 3.对DS中数据进行结构的转换      String-->Json
        //这种方式可以,这种方式如果里面有泛型,会出现泛型的擦除
//        SingleOutputStreamOperator<JSONObject> jsonObjectDataStream = kafkasourceDataStream.map(line -> JSON.parseObject(line));
        //TODO 这种String转为JSON的方式也可以
        SingleOutputStreamOperator<JSONObject> jsonObjectDataStream = kafkasourceDataStream.map(JSON::parseObject);
        //TODO 4.对数据进行ETL   如果table为空 或者 data为空，或者长度<3,将这样的数据过滤掉
        SingleOutputStreamOperator<JSONObject> filterDataStream = jsonObjectDataStream.filter(
                line -> {
                    boolean flag = line.getString("table") != null && line.getJSONObject("data") != null && line.getString("data").length() > 3;
                    return flag;
                }
        );

        //定义侧输出流标签,{}防止泛型擦除
        OutputTag<JSONObject> hbaseOutputTag = new OutputTag<JSONObject>(TableProcess.SINK_TYPE_HBASE){};

        //TODO 5. 动态分流  事实表放到主流，写回到kafka的DWD层；如果维度表，通过侧输出流，写入到Hbase
        //从mysql数据库中查询配置表的信息,但是我放在processElement中,每条数据来了都查询一次,这样不好.
        //但是我放在open方法中,我只查询一次,这样也不好.以为我open只查询一次后,放入内存中. 一旦我配置表发生变化就无法知道了.
        //所以我想你各一段时间查询一次.写一个定时任务.每隔5秒查询一次.

        //万能的process算子,输出侧输出流.这里属于海量数据做处理.而open中只差一次
        //主流,输出到kafka的DWD层. 是事实表
        SingleOutputStreamOperator<JSONObject> mainkafkaDataStream  = filterDataStream.process(new TableProcessFunction(hbaseOutputTag));
        //侧输出流,输出到Hbase中,是维度表
        DataStream<JSONObject> sideOutputHbaseDataStream = mainkafkaDataStream.getSideOutput(hbaseOutputTag);

        //测试打印输出
        mainkafkaDataStream.print("事实表");
        sideOutputHbaseDataStream.print("维度表");

        //TODO 6.将维度数据保存到Phoenix对应的维度表中
        sideOutputHbaseDataStream.addSink(new PhoenixSinkFunction());

        //TODO 7.将事实数据写回到kafka的dwd层
        mainkafkaDataStream.addSink(MyKafkaUtil.getKafkaSinkBySchema(
                new KafkaSerializationSchema<JSONObject>() {
                    @Override
                    public void open(SerializationSchema.InitializationContext context) throws Exception {
                        System.out.println("kafka自定义序列化发送到kafka不同主题上");
                    }
                    /**示列:
                     * {"database":"gmall",table":"refund payment","type":"insert","ts": 1612249813, xid": 479, offset": 2762,
                     * "data":{id:2786,"out_trade_no": 177831971567712","order id: 28769," sku_id":8, payment_type": 1101","trade_no":null,"totalamount":16394.0," subject":"退款"," refund_status":"7020"," create_time":"2021-2-0215:10:13","ca11 back time":"2021-92-9215:10:13","ca11 back content":nu11}
                     * }
                     */
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, @Nullable Long aLong) {
                        //sinkTopic使我们指定的主题, 如果这个主题为null,他发动到默认主题中.
                        String sinkTopic = jsonObject.getString("sink_table");
                        JSONObject dataJsonObj = jsonObject.getJSONObject("data");
                        //TODO 什么叫序列化? 就是将数据转换成字节的过程叫序列化
                        //<>这个里面的对象你可以加,不加它可以推断出来.这叫砖石表达式,从1.5之后,泛型里面的东西可以不加了.
                        return new ProducerRecord<>(sinkTopic, dataJsonObj.toString().getBytes());//注意将json转为字节数组
                    }
                }
        ));
        env.execute();
    }
}
