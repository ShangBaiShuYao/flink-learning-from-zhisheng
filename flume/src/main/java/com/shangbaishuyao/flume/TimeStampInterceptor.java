package com.shangbaishuyao.flume;

import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
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

public class TimeStampInterceptor implements Interceptor {

    private ArrayList<Event> events = new ArrayList<>();

    @Override
    public void initialize() {}

    @Override
    public Event intercept(Event event) {

        Map<String, String> headers = event.getHeaders();
        String log = new String(event.getBody(), StandardCharsets.UTF_8);

        JSONObject jsonObject = JSONObject.parseObject(log);

        String ts = jsonObject.getString("ts");
        headers.put("timestamp", ts);

        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        events.clear();
        for (Event event : list) {
            events.add(intercept(event));
        }
        return events;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {
        @Override
        public Interceptor build() {
            return new TimeStampInterceptor();
        }

        @Override
        public void configure(Context context) {
        }
    }
}
