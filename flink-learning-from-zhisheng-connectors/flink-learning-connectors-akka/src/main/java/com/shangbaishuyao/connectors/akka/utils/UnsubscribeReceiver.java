package com.shangbaishuyao.connectors.akka.utils;

import akka.actor.ActorRef;

import java.io.Serializable;

/**
 * Desc: General interface used by Receiver Actor to un subscribe.(接收方参与者用于取消订阅的通用接口)
 *
 * 源码: https://www.codota.com/web/assistant/code/rs/5c660aa61095a500019be2cb#L44
 *
 * @Author: 上白书妖
 * @Date: 14:12 2021/1/19
 */
public class UnsubscribeReceiver implements Serializable {
    private static final long serialVersionUID = 1L;
    private ActorRef receiverActor;

    public UnsubscribeReceiver(ActorRef receiverActor) {
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
        if (obj instanceof UnsubscribeReceiver) {
            UnsubscribeReceiver other = (UnsubscribeReceiver) obj;
            return other.canEquals(this) && super.equals(other)
                    && receiverActor.equals(other.getReceiverActor());
        } else {
            return false;
        }
    }

    public boolean canEquals(Object obj) {
        return obj instanceof UnsubscribeReceiver;
    }
}
