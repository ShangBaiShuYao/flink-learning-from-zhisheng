package com.shangbaishuyaoconnectors.rocketmq;

import com.shangbaishuyaoconnectors.rocketmq.common.serialization.KeyValueDeserializationSchema;
import org.apache.commons.lang3.Validate;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.rocketmq.client.consumer.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.shangbaishuyaoconnectors.rocketmq.RocketMQUtils.getLong;

/**
 * Desc: RocketMQSource <OUT> extends RichParallelSourceFunction<OUT> <br/>
 *
 * 知识补充: <br/>
 *  func 这个委托的in和out，就是字面意思，可能我需要给你举个例：
 *
 * 有个类型A：
 * class A
 * {
 * public name{get;set;}
 * }
 *
 * 某个函数中
 * 有一个List<A> lst_A;
 * 现在我需要得到lst_A中的name的list，我要这样做：
 * Func<A,string> selector = new Func<A,string>(a=>a.name);
 * List<string> lst_name = lst_A.select<A,string>(selector).ToList();
 *
 * 可以简化为
 * List<string> lst_name = lst_A.select<A,string>(a=>a.name).tolist();
 *
 * 这样看懂了吗？？
 * 对于func来说，传入的类型是A，但是out传出的类型是string
 *
 * create by shangbaishuyao on 2021-01-24
 * @Author: 上白书妖
 * @Date: 13:38 2021/1/24
 */
public class RocketMQSource<OUT> extends RichParallelSourceFunction<OUT>
        implements CheckpointedFunction , ResultTypeQueryable<OUT> {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RocketMQSource.class);


    private transient MQPullConsumerScheduleService pullConsumerScheduleService;
    private DefaultMQPullConsumer consumer;

    private KeyValueDeserializationSchema<OUT> schema;

    private RunningChecker runningChecker;

    private transient ListState<Tuple2<MessageQueue, Long>> unionOffsetStates;
    private Map<MessageQueue, Long> offsetTable;
    private Map<MessageQueue, Long> restoredOffsets;

    private Properties props;
    private String topic;
    private String group;

    private static final String OFFSETS_STATE_NAME = "topic-partition-offset-states";

    private transient volatile boolean restored;


    public RocketMQSource(KeyValueDeserializationSchema<OUT> schema, Properties props) {
        this.schema = schema;
        this.props = props;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        LOG.debug("source open ...");
        //判空
        Validate.notEmpty(props,"Consumer properties can not be empty");
        Validate.notNull(schema,"KeyValueDeserializationSchema can not be null");

        //通过key得到value
        this.topic = props.getProperty(RocketMQConfig.CONSUMER_TOPIC);//消费者主题
        this.group = props.getProperty(RocketMQConfig.CONSUMER_GROUP);//消费者组

        //判空
        Validate.notEmpty(topic,"Consumer topic can not be empty");
        Validate.notEmpty(group,"Consumer group can not be empty");

        //初始化
        if(offsetTable == null){
            offsetTable = new ConcurrentHashMap<>();
        }
        if (restoredOffsets == null) {
            restoredOffsets = new ConcurrentHashMap<>();
        }
        runningChecker = new RunningChecker();

        //通过消费者组获取Default pulling consumer
        pullConsumerScheduleService = new MQPullConsumerScheduleService(group);
        consumer = pullConsumerScheduleService.getDefaultMQPullConsumer();

        //将上下文运行环的子任务的作为实例名
        consumer.setInstanceName(String.valueOf(getRuntimeContext().getIndexOfThisSubtask()));
        //构建 Consumer 配置
        RocketMQConfig.buildConsumerConfigs(props,consumer);

    }

    @Override
    public void run(SourceContext<OUT> context) throws Exception {
        LOG.debug("source run....");
        // The lock that guarantees that record emission and state updates are atomic,from the view of taking a checkpoint.
        //从获取检查点的角度来看，保证记录释放和状态更新的锁是原子的。
        final Object lock = context.getCheckpointLock();

        //当消息延迟没有被发现 则是默认值 10
        //如果通过key得不到value的话,则将默认值返回,如果没有值则相当于是props.getProperty(10);
        int delayWhenMessageNotFound = RocketMQUtils.getInteger(props,
                //当没有找到消息时，用户延迟
                RocketMQConfig.CONSUMER_DELAY_WHEN_MESSAGE_NOT_FOUND,
                //当没有找到消息时，用户延迟,则默认值是10
                RocketMQConfig.DEFAULT_CONSUMER_DELAY_WHEN_MESSAGE_NOT_FOUND);

        //设置标签
        //如果能通过标签获取值则设置标签获取的值 ,否则 设置默认的消费者标签
        String tag = props.getProperty(RocketMQConfig.CONSUMER_TAG, RocketMQConfig.DEFAULT_CONSUMER_TAG);

        //设置消费者拉去线程池的大小
        //如果没有,则这是默认的20
        int pullPoolSize = RocketMQUtils.getInteger(props, RocketMQConfig.CONSUMER_PULL_POOL_SIZE,
                RocketMQConfig.DEFAULT_CONSUMER_PULL_POOL_SIZE);

        //设置消费者批量拉取的大小
        int pullBatchSize = RocketMQUtils.getInteger(props, RocketMQConfig.CONSUMER_BATCH_SIZE,
                RocketMQConfig.DEFAULT_CONSUMER_BATCH_SIZE);

        //设置拉取的线程数量
        pullConsumerScheduleService.setPullThreadNums(pullPoolSize);

                                                            //主题   拉取任务回调函数做参数,里面是消息队列和拉取任务上下文做参数组成整个回调函数对象
        pullConsumerScheduleService.registerPullTaskCallback(topic, new PullTaskCallback() {
            @Override
            public void doPullTask(MessageQueue mq, PullTaskContext pullTaskContext) {
                try {
                    //获取消息队列偏移量
                    long offset = getMessageQueueOffset(mq);
                    if (offset < 0) {
                        return;
                    }
                              //拉取结果                  //消息队列对象 标签 偏移量  批量拉取大小
                    PullResult pullResult = consumer.pull(mq, tag, offset, pullBatchSize);

                    boolean found = false;

                    switch (pullResult.getPullStatus()) {
                        case FOUND:
                            List<MessageExt> messages = pullResult.getMsgFoundList();
                            for (MessageExt msg : messages) {
                                byte[] key = msg.getKeys() != null ? msg.getKeys().getBytes(StandardCharsets.UTF_8) : null;
                                byte[] value = msg.getBody();
                                OUT data = schema.deserializeKeyAndValue(key, value);

                                // output and state update are atomic
                                synchronized (lock) {
                                    context.collectWithTimestamp(data, msg.getBornTimestamp());
                                }
                            }
                            found = true;
                            break;
                        case NO_MATCHED_MSG:
                            LOG.debug("No matched message after offset {} for queue {}", offset, mq);
                            break;
                        case NO_NEW_MSG:
                            break;
                        case OFFSET_ILLEGAL:
                            LOG.warn("Offset {} is illegal for queue {}", offset, mq);
                            break;
                        default:
                            break;
                    }
                }catch (Exception e){
                    throw new RuntimeException(e);
                }
            }
        });



        try {
            pullConsumerScheduleService.start();
        } catch (MQClientException e) {
            throw new RuntimeException(e);
        }

        runningChecker.setRunning(true);

        awaitTermination();

    }

    /**
     * Desc: 等待结束
     * @throws InterruptedException
     */
    private void awaitTermination() throws InterruptedException {
        while (runningChecker.isRunning()) {
            Thread.sleep(50);
        }
    }
    /**
     * Desc: 获取消息队列偏移量
     *
     * Class class MessageQueue implements Comparable<MessageQueue>{
     *     private String topic;  //主题
     *     private String brokerName;
     *     private int queueId;  //队列ID号
     *     }
     * @param mq
     * @return
     * @throws MQClientException
     */
    private long getMessageQueueOffset(MessageQueue mq) throws MQClientException {
        Long offset = offsetTable.get(mq);
        // restoredOffsets(unionOffsetStates) is the restored global union state;(restoredOffsets(unionOffsetStates)是恢复的全局union状态;)
        // should only snapshot mqs that actually belong to us ;(事实上只是一个快照消息)
        if (restored && offset == null) {
            // private Map<MessageQueue, Long> restoredOffsets;
            offset = restoredOffsets.get(mq);
        }
        //偏移量不为空时
        if (offset == null) {
            //获取消费者偏移量
            offset = consumer.fetchConsumeOffset(mq, false);
            if (offset < 0) {                                           //消费者补偿重置                 消费者抵消最新
                String initialOffset = props.getProperty(RocketMQConfig.CONSUMER_OFFSET_RESET_TO, RocketMQConfig.CONSUMER_OFFSET_LATEST);
                switch (initialOffset) {
                    case RocketMQConfig.CONSUMER_OFFSET_EARLIEST: offset = consumer.minOffset(mq);
                        break;
                    case RocketMQConfig.CONSUMER_OFFSET_LATEST: offset = consumer.maxOffset(mq);
                        break;
                    case RocketMQConfig.CONSUMER_OFFSET_TIMESTAMP: offset = consumer.searchOffset(mq, getLong(props, RocketMQConfig.CONSUMER_OFFSET_FROM_TIMESTAMP, System.currentTimeMillis()));
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown value for CONSUMER_OFFSET_RESET_TO.");
                }
            }
        }
        offsetTable.put(mq, offset);
        return offsetTable.get(mq);
    }

    /**
     *  Desc: 放置消息队列偏移量
     * @param mq
     * @param offset
     * @throws MQClientException
     */
    private void putMessageQueueOffset(MessageQueue mq, long offset) throws MQClientException {
        offsetTable.put(mq, offset);
        consumer.updateConsumeOffset(mq, offset);
    }


    @Override
    public void cancel() {
        LOG.debug("cancel ...");
        runningChecker.setRunning(false);

        if (pullConsumerScheduleService != null) {
            pullConsumerScheduleService.shutdown();
        }

        offsetTable.clear();
        restoredOffsets.clear();
    }


    @Override
    public void close() throws Exception {
        LOG.debug("close ...");
        try {
            cancel();
        } finally {
            super.close();
        }
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return schema.getProducedType();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
// called when a snapshot for a checkpoint is requested

        if (!runningChecker.isRunning()) {
            LOG.debug("snapshotState() called on closed source; returning null.");
            return;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Snapshotting state {} ...", context.getCheckpointId());
        }

        unionOffsetStates.clear();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Snapshotted state, last processed offsets: {}, checkpoint id: {}, timestamp: {}",
                    offsetTable, context.getCheckpointId(), context.getCheckpointTimestamp());
        }

        // remove the unassigned queues in order to avoid read the wrong offset when the source restart
        Set<MessageQueue> assignedQueues = consumer.fetchMessageQueuesInBalance(topic);
        offsetTable.entrySet().removeIf(item -> !assignedQueues.contains(item.getKey()));

        for (Map.Entry<MessageQueue, Long> entry : offsetTable.entrySet()) {
            unionOffsetStates.add(Tuple2.of(entry.getKey(), entry.getValue()));
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
// called every time the user-defined function is initialized,
        // be that when the function is first initialized or be that
        // when the function is actually recovering from an earlier checkpoint.
        // Given this, initializeState() is not only the place where different types of state are initialized,
        // but also where state recovery logic is included.
        LOG.debug("initialize State ...");
        this.unionOffsetStates = context.getOperatorStateStore().getUnionListState(new ListStateDescriptor<>(
                OFFSETS_STATE_NAME, TypeInformation.of(new TypeHint<Tuple2<MessageQueue, Long>>() {
        })));

        this.restored = context.isRestored();

        if (restored) {
            if (restoredOffsets == null) {
                restoredOffsets = new ConcurrentHashMap<>();
            }
            for (Tuple2<MessageQueue, Long> mqOffsets : unionOffsetStates.get()) {
                if (!restoredOffsets.containsKey(mqOffsets.f0) || restoredOffsets.get(mqOffsets.f0) < mqOffsets.f1) {
                    restoredOffsets.put(mqOffsets.f0, mqOffsets.f1);
                }
            }
            LOG.info("Setting restore state in the consumer. Using the following offsets: {}", restoredOffsets);
        } else {
            LOG.info("No restore state for the consumer.");
        }
    }
}
