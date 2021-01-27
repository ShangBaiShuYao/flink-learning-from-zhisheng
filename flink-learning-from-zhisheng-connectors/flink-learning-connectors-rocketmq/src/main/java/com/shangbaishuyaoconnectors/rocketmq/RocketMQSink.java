package com.shangbaishuyaoconnectors.rocketmq;

import com.shangbaishuyaoconnectors.rocketmq.common.selector.TopicSelector;
import com.shangbaishuyaoconnectors.rocketmq.common.serialization.KeyValueSerializationSchema;
import org.apache.commons.lang3.Validate;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

/**
 * Desc:
 * created by shangbaishuyao on 2021-01-19
 * @Author: 上白书妖
 * @Date: 20:29 2021/1/19
 */
public class RocketMQSink<IN> extends RichSinkFunction<IN>  implements CheckpointedFunction {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(RocketMQSink.class);

    private transient DefaultMQProducer producer;
    private boolean async; // false by default

    private Properties props;
    private TopicSelector<IN> topicSelector;
    private KeyValueSerializationSchema<IN> serializationSchema;

    private boolean batchFlushOnCheckpoint; // false by default
    private int batchSize = 1000;
    private List<Message> batchList;

    private int messageDeliveryDelayLevel = RocketMQConfig.MSG_DELAY_LEVEL00;

    public RocketMQSink(KeyValueSerializationSchema<IN> schema,TopicSelector<IN> topicSelector,Properties props){
        this.serializationSchema = schema;
        this.topicSelector = topicSelector;
        this.props = props;

        if (props != null){
            //消息递送延迟水平
            this.messageDeliveryDelayLevel = RocketMQUtils.getInteger(this.props,RocketMQConfig.MSG_DELAY_LEVEL,RocketMQConfig.MSG_DELAY_LEVEL00);
            if (this.messageDeliveryDelayLevel < RocketMQConfig.MSG_DELAY_LEVEL00){
                this.messageDeliveryDelayLevel = RocketMQConfig.MSG_DELAY_LEVEL00;
            }else if (this.messageDeliveryDelayLevel > RocketMQConfig.MSG_DELAY_LEVEL18){
                this.messageDeliveryDelayLevel = RocketMQConfig.MSG_DELAY_LEVEL18;
            }
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //his class assists in validating arguments (他的类帮助验证参数)
        Validate.notEmpty(props,"Producer properties can not be empty(生产者属性不能为空)");
        Validate.notNull(topicSelector,"TopicSelector can not be null(TopicSelector不能为空)");
        Validate.notNull(serializationSchema,"KeyValueSerializationSchema can not be null(KeyValueSerializationSchema不能为空)");

        producer = new DefaultMQProducer();
        //默认生产者设置实例名称,根据子任务的编号
        producer.setInstanceName(String.valueOf(getRuntimeContext().getIndexOfThisSubtask()));//获取此并行子任务的编号。编号从0开始，一直到parallelism-1
        //构建 producer 配置
        RocketMQConfig.buildProducerConfigs(props,producer);

        //批次列表
        batchList = new LinkedList<>();
                                                             //isCheckpointingEnabled(): 如果为正在运行的作业启用检查点，则返回true
        if (batchFlushOnCheckpoint && !((StreamingRuntimeContext) getRuntimeContext()).isCheckpointingEnabled()) {
            LOG.warn("Flushing on checkpoint is enabled, but checkpointing is not enabled. Disabling flushing.(启用检查点上的刷新，但不启用检查点。禁用冲洗)");
            batchFlushOnCheckpoint = false;
        }

        try {
            producer.start();
        } catch (MQClientException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     *  解析消息<br/>
     *
     * @param input
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(IN input, Context context) throws Exception {
        Message msg = prepareMessage(input);
    }

    /**
     * 解析消息
     * @param input
     * @return
     */
    private Message prepareMessage(IN input) {
        String topic = topicSelector.getTopic(input);

        return null;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {

    }
}
