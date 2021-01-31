package com.shangbaishuyao.connectors.flume;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flume.Event;

import java.io.Serializable;

/*
 * Desc:  A function that can create a Event from an incoming instance of the given type.可以从给定类型的传入实例创建事件的函数 <br/>
 * create by shangbaishuyao on 2021/1/31
 * @Author: 上白书妖
 * @Date: 13:30 2021/1/31
 */
public interface FlumeEventBuilder<IN> extends Function, Serializable {
    Event createFlumeEvent(IN value, RuntimeContext ctx);
}
