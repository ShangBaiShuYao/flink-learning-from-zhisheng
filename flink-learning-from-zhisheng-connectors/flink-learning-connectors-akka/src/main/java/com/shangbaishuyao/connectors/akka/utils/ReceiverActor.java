package com.shangbaishuyao.connectors.akka.utils;

import akka.actor.ActorSelection;
import akka.actor.UntypedActor;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * Desc:
 *
 * 源码: https://github.com/apache/bahir-flink/blob/master/flink-connector-akka/src/main/java/org/apache/flink/streaming/connectors/akka/utils/ReceiverActor.java#L57
 *
 * create by shangbaishuyao 2021-01-19
 * @Author: 上白书妖
 * @Date: 13:43 2021/1/19
 */
//继承scala类,重写方法
public class ReceiverActor extends UntypedActor {
    // --- Fields set by the constructor(由构造函数设置的字段)
    private final SourceFunction.SourceContext<Object> ctx;
    private final String urlOfPublisher;
    private final boolean autoAck;

    // --- Runtime fields(运行时字段)
    private ActorSelection remotePublisher;

    //构造方法
    public ReceiverActor(SourceFunction.SourceContext<Object> ctx,
                         String urlOfPublisher,
                         boolean autoAck) {
        this.ctx = ctx;
        this.urlOfPublisher = urlOfPublisher;
        this.autoAck = autoAck;
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        remotePublisher = getContext().actorSelection(urlOfPublisher);
        remotePublisher.tell(new SubscribeReceiver(getSelf()), getSelf());
    }

    @Override
    public void onReceive(Object message) throws Exception {

    }
}
