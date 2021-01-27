package com.shangbaishuyaoconnectors.rocketmq.common.serialization;

import java.io.Serializable;

/**
 * Desc:
 * created by shangbaishuyao on 2021-01-19
 * @Author: 上白书妖
 * @Date: 15:45 2021/1/19
 */
public interface KeyValueSerializationSchema<T> extends Serializable {
    byte[] serializeKey(T tuple);

    byte[] serializeValue(T tuple);
}
