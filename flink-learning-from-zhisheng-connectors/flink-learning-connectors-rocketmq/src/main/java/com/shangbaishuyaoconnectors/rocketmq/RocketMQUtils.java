package com.shangbaishuyaoconnectors.rocketmq;

import java.util.Properties;

/**
 * Desc: RocketMQ 工具类 <br/>
 * 源码地址: import static org.apache.storm.rocketmq.RocketMqUtils;
 * @Author: 上白书妖
 * @Date: 17:24 2021/1/19
 */
public class RocketMQUtils {
    public static int getInteger(Properties props, String key, int defaultValue) {
                                    //如果通过key得不到value的话,则将默认值返回
                                    //这其实就是看你配置层面里面有没有设置了,如果配置层面没有配置的话,就直接使用代码层面配置
        return Integer.parseInt(props.getProperty(key, String.valueOf(defaultValue)));
    }

    public static long getLong(Properties props, String key, long defaultValue) {
        return Long.parseLong(props.getProperty(key, String.valueOf(defaultValue)));
    }

    public static boolean getBoolean(Properties props, String key, boolean defaultValue) {
        return Boolean.parseBoolean(props.getProperty(key, String.valueOf(defaultValue)));
    }
}
