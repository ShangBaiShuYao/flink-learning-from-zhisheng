package com.shangbaishuyao.flume;

import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;

/**
 * @author shangbaishuyao
 * @create 2021-11-14 下午1:46
 */

public class ETLInterceptor2 implements Interceptor {

    @Override
    public void initialize() {}

    @Override
    public Event intercept(Event event) {
        return null;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        return null;
    }

    @Override
    public void close() {

    }
}
