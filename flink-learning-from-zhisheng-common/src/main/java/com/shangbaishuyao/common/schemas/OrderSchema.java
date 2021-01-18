package com.shangbaishuyao.common.schemas;

import com.google.gson.Gson;
import com.shangbaishuyao.common.model.OrderEvent;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.nio.charset.Charset;

/**
 * order Schema ，支持序列化和反序列化<br/>
 *
 *@Author: 上白书妖
 *@Date: 2020/11/16 14:17
 */
public class OrderSchema implements DeserializationSchema<OrderEvent>, SerializationSchema<OrderEvent> {
    private static  final Gson gson = new Gson();

    @Override
    public OrderEvent deserialize(byte[] bytes) throws IOException {
        return gson.fromJson(new String(bytes), OrderEvent.class);
    }

    @Override
    public boolean isEndOfStream(OrderEvent orderEvent) {
        return false;
    }

    @Override
    public byte[] serialize(OrderEvent orderEvent) {
        return gson.toJson(orderEvent).getBytes(Charset.forName("UTF-8"));
    }

    @Override
    public TypeInformation<OrderEvent> getProducedType() {
        return TypeInformation.of(OrderEvent.class);
    }
}
