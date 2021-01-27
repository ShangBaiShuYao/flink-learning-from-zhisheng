package com.shangbaishuyaoconnectors.rocketmq.common.selector;

/**
 * Desc: 默认主题选择器 <br/>
 *
 * created by shangbaishuyao on 2021-01-19
 * @Author: 上白书妖
 * @Date: 15:03 2021/1/19
 */
public class DefaultTopicSelector<T>  implements TopicSelector<T>{

    private final String topicName; //主题名
    private final String tagName; //标签名

    public DefaultTopicSelector(final String topicName) {
        this(topicName, "");
    }

    //构造
    public DefaultTopicSelector(String topicName, String tagName) {
        this.topicName = topicName;
        this.tagName = tagName;
    }


    @Override
    public String getTopic(T tuple) {
        return topicName;
    }

    @Override
    public String getTag(T tuple) {
        return tagName;
    }
}
