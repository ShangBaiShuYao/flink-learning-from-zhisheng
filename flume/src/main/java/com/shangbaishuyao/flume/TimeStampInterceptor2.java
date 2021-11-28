package com.shangbaishuyao.flume;


import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * <p>消费者 flume</p>
 *  file文件 --> flume --> kafka -- flume   此为 kafka --> flume 部分 <br/>
 * 解决的思路：拦截json日志，通过fastjson框架解析json，获取实际时间ts。
 * 将获取的ts时间写入拦截器header头，header的key必须是timestamp，因为Flume框架会根据这个key的值识别为时间，写入到HDFS。
 * @author shangbaishuyao
 * @create 2021-11-14 下午3:21
 */

public class TimeStampInterceptor2 implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {

        Map<String, String> headers = event.getHeaders();
        String log = new String(event.getBody(), StandardCharsets.UTF_8);

        JSONObject jsonObject = JSONObject.parseObject(log);

        Long ts = jsonObject.getLong("ts");

        //Maxwell输出的数据中的ts字段时间戳单位为秒，Flume HDFSSink要求单位为毫秒
        String timeMills = String.valueOf(ts * 1000);

        headers.put("timestamp", timeMills);

        return event;

    }

    @Override
    public List<Event> intercept(List<Event> events) {
        for (Event event : events) {
            intercept(event);
        }
        return events;
    }

    @Override
    public void close() {}

    public static class Builder implements Interceptor.Builder {
        @Override
        public Interceptor build() {
            return new TimeStampInterceptor2();
        }
        @Override
        public void configure(Context context) {}
    }
}
