package com.shangbaishuyao.common.model;

import java.util.Map;

/**
 * Desc:度量事件<br/>
 *
 *@Author: 上白书妖
 *@Date: 2020/11/13 11:27
 */
public class MetricEvent {

    /**
     * Metric name : 度量名字
     */
    private String name;

    /**
     * Metric timestamp : 度量时间戳
     */
    private Long timestamp;

    /**
     * Metric fields
     */
    private Map<String, Object> fields;

    /**
     * Metric tags :度量标签
     */
    private Map<String, String> tags;


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Map<String, Object> getFields() {
        return fields;
    }

    public void setFields(Map<String, Object> fields) {
        this.fields = fields;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }
}
