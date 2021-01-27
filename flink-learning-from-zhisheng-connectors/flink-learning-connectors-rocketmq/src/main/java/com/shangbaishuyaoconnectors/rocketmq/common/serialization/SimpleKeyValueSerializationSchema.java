package com.shangbaishuyaoconnectors.rocketmq.common.serialization;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Desc:
 * created by shangbaishuyao on 2021-01-19
 * @Author: 上白书妖
 * @Date: 16:16 2021/1/19
 */
public class SimpleKeyValueSerializationSchema implements KeyValueSerializationSchema<Map>{
    public static final String DEFAULT_KEY_FIELD = "key";
    public static final String DEFAULT_VALUE_FIELD = "value";

    public String keyField;
    public String valueField;

    //构造函数
    public SimpleKeyValueSerializationSchema() {
        this(DEFAULT_KEY_FIELD,DEFAULT_VALUE_FIELD);
    }

    //构造函数
    public SimpleKeyValueSerializationSchema(String keyField, String valueField) {
        this.keyField = keyField;
        this.valueField = valueField;
    }


    @Override
    public byte[] serializeKey(Map tuple) {
        return getBytes(tuple,keyField);
    }

    @Override
    public byte[] serializeValue(Map tuple) {
        return getBytes(tuple,valueField);
    }


    private byte[] getBytes(Map tuple,String key){
        if (tuple == null || key == null){
            return null;
        }
        //根据key获取value值
        Object value = tuple.get(key);
        return value != null ? value.toString().getBytes(StandardCharsets.UTF_8) : null;
    }
}
