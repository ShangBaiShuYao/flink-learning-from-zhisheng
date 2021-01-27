package com.shangbaishuyao.connectors.akka.utils;

import akka.actor.ActorRef;

import java.io.Serializable;

/**
 * Desc: General interface used by Receiver Actor to subscribe to the publisher.(接收方Actor用于订阅发布者的通用接口)
 *
 *  org.apache.flink.streaming.connectors.akka.utils源码
 *
 * 源码: https://github.com/apache/bahir-flink/blob/master/flink-connector-akka/src/main/java/org/apache/flink/streaming/connectors/akka/utils/SubscribeReceiver.java#L44
 *
 * @Author: 上白书妖
 * @Date: 14:06 2021/1/19
 */
public class SubscribeReceiver implements Serializable {
    private static final long serialVersionUID = 1L;
    private ActorRef receiverActor;

    public SubscribeReceiver(ActorRef receiverActor) {
        this.receiverActor = receiverActor;
    }

    public void setReceiverActor(ActorRef receiverActor) {
        this.receiverActor = receiverActor;
    }

    public ActorRef getReceiverActor() {
        return receiverActor;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof SubscribeReceiver) {
            SubscribeReceiver other = (SubscribeReceiver) obj;
            return other.canEquals(this) && super.equals(other) && receiverActor.equals(other.getReceiverActor());
        } else {
            return false;
        }
    }

    public boolean canEquals(Object obj) {
        return obj instanceof SubscribeReceiver;
    }
}
