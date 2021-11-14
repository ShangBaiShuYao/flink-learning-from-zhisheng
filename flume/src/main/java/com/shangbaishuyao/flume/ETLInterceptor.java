package com.shangbaishuyao.flume;

import com.shangbaishuyao.Utils.JSONUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

/**
 * <p>生产者 flume</p>
 * file文件 --> flume --> kafka -- flume   此为 file文件 --> flume --> kafka部分 <br/>
 * flume 拦截器, 简单判断json字符串是否完整. 不完整的舍弃.
 * @author shangbaishuyao
 * @create 2021-11-14 下午1:41
 */

public class ETLInterceptor implements Interceptor {

    @Override
    public void initialize() {}

    @Override
    public Event intercept(Event event) {

        byte[] body = event.getBody();
        String log = new String(body, StandardCharsets.UTF_8);

        if (JSONUtils.isJSONValidate(log)) {
            return event;
        } else {
            return null;
        }
    }

    @Override
    public List<Event> intercept(List<Event> list) {

        Iterator<Event> iterator = list.iterator();

        while (iterator.hasNext()){
            Event next = iterator.next();
            if(intercept(next)==null){
                iterator.remove();
            }
        }
        return list;
    }

    public static class Builder implements Interceptor.Builder{
        @Override
        public Interceptor build() {
            return new ETLInterceptor();
        }

        @Override
        public void configure(Context context) {}
    }

    @Override
    public void close() {}
}
