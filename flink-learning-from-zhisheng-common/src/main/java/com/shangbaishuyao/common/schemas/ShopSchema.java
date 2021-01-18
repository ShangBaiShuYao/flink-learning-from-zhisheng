package com.shangbaishuyao.common.schemas;

import com.google.gson.Gson;
import com.shangbaishuyao.common.model.ShopEvent;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.nio.charset.Charset;

/**
 *Shop Schema ，支持序列化和反序列化<br/>
 *
 *@Author: 上白书妖
 *@Date: 2020/11/16 14:27
 */
public class ShopSchema implements DeserializationSchema<ShopEvent>, SerializationSchema<ShopEvent> {

    private static final Gson gson = new Gson();

    @Override
    public ShopEvent deserialize(byte[] bytes) throws IOException {
        return gson.fromJson(new String(bytes),ShopEvent.class);
    }

    @Override
    public boolean isEndOfStream(ShopEvent shopEvent) {
        return false;
    }

    @Override
    public byte[] serialize(ShopEvent shopEvent) {
        return gson.toJson(shopEvent).getBytes(Charset.forName("UTF-8"));
    }

    @Override
    public TypeInformation<ShopEvent> getProducedType() {
        return TypeInformation.of(ShopEvent.class);
    }
}
