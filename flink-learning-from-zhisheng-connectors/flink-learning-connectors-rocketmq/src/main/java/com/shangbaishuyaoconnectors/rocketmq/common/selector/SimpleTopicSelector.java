package com.shangbaishuyaoconnectors.rocketmq.common.selector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Desc: 简单的主题选择器 <br/>
 *
 * created by shangbaishuyao on 2021-01-19
 * @Author: 上白书妖
 * @Date: 15:21 2021/1/19
 */
public class SimpleTopicSelector implements TopicSelector<Map>{
    private static final Logger LOG = LoggerFactory.getLogger(SimpleTopicSelector.class);

    private final String topicFieldName;  //主题字段名
    private final String defaultTopicName; //默认主题名

    private final String tagFieldName; //标签字段名
    private final String defaultTagName; //默认标签名

    public SimpleTopicSelector(String topicFieldName, String defaultTopicName, String tagFieldName, String defaultTagName) {
        this.topicFieldName = topicFieldName;
        this.defaultTopicName = defaultTopicName;
        this.tagFieldName = tagFieldName;
        this.defaultTagName = defaultTagName;
    }

    @Override
    public String getTopic(Map tuple) {
        //判断是否包含指定的键名
        if(tuple.containsKey(topicFieldName)){
            Object topic = tuple.get(topicFieldName);
            return topic != null? topic.toString() : defaultTopicName;
        }else {
            LOG.warn("Field {} Not Found. Returning default topic {}/字段{}未找到。返回默认主题{}", topicFieldName, defaultTopicName);
            return defaultTopicName;
        }
    }

    @Override
    public String getTag(Map tuple) {
        if (tuple.containsKey(tagFieldName)){
            Object tag = tuple.get(tagFieldName);
            return tag != null ? tag.toString() : defaultTagName;
        }else{
            LOG.warn("Field {} Not Found. Returning default tag {}/字段{}未找到。返回默认标签{}", tagFieldName, defaultTagName);
            return defaultTagName;
        }
    }
}
