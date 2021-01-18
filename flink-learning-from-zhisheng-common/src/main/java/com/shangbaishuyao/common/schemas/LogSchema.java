package com.shangbaishuyao.common.schemas;

import com.google.gson.Gson;
import com.shangbaishuyao.common.model.LogEvent;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.nio.charset.Charset;

/**
 * Desc: 日志主题<br/>
 * Log Schema ，支持序列化和反序列化<br/>
 *
 *@Author: 上白书妖
 *@Date: 2020/11/14 14:34
 */
public class LogSchema implements DeserializationSchema<LogEvent>, SerializationSchema<LogEvent> {

    private static final Gson gson = new Gson();


    @Override
    public LogEvent deserialize(byte[] bytes) throws IOException {
        LogEvent logEvent = gson.fromJson(new String(bytes), LogEvent.class);
        return logEvent;
    }

    @Override
    public boolean isEndOfStream(LogEvent logEvent) {
        return false;
    }

    @Override
    public byte[] serialize(LogEvent logEvent) {
        return gson.toJson(logEvent).getBytes(Charset.forName("UTF-8"));
    }

    @Override
    public TypeInformation<LogEvent> getProducedType() {
        return TypeInformation.of(LogEvent.class);
    }
}
