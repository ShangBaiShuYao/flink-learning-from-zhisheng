package com.shangbaishuyao.common.model;

import java.util.HashMap;
import java.util.Map;

/**
 * Desc:日志事件<br/>
 *
 *@Author: 上白书妖
 *@Date: 2020/11/13 11:09
 */

//@Data
//@Builder 使用了@Bulider和@Data注解后，就可以使用链式风格优雅地创建对象; 但是LomBok依赖性很强,所以这里不使用LomBok插件
public class LogEvent {
    //the type of log(app、docker、...):日志类型
    private String type;

    // the timestamp of log
    private Long timestamp;

    //the level of log(debug/info/warn/error):日志级别
    private String level;

    //the message of log:消息日志
    private String message;

    //the tag of log(appId、dockerId、machine hostIp、machine clusterName、...):日志标签
    private Map<String, String> tags = new HashMap<>();

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }
}
