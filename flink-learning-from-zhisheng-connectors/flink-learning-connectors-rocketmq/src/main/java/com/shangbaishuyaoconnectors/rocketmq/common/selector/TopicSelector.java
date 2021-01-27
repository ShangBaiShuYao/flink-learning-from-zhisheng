package com.shangbaishuyaoconnectors.rocketmq.common.selector;

import java.io.Serializable;

/**
 * Desc: 主题选择器 <br/>
 * created by shangbaishuyao on 2021-01-19
 * @Author: 上白书妖
 * @Date: 15:13 2021/1/19
 */
public interface TopicSelector<T> extends Serializable {

    String getTopic(T tuple);

    String getTag(T tuple);
}
