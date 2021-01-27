package com.shangbaishuyaoconnectors.rocketmq.common.serialization;

import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

import java.io.Serializable;

/**
 * Desc: key,value 反序列化主题 <br/>
 * created by shangbaishuyao on 2021-01-19
 * @Author: 上白书妖
 * @Date: 15:40 2021/1/19
 */
public interface KeyValueDeserializationSchema<T> extends ResultTypeQueryable<T> , Serializable {

    T deserializeKeyAndValue(byte[] key, byte[] value);
}
