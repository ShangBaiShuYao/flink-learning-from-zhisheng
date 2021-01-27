package com.shangbaishuyaoconnectors.rocketmq.common.serialization;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Desc: 简便的key,value反序列化主题 <br/>
 * created by shangbaishuyao on 2021-01-19
 * @Author: 上白书妖
 * @Date: 15:46 2021/1/19
 */
public class SimpleKeyValueDeserializationSchema implements KeyValueDeserializationSchema<Map>{

    public static final String DEFAULT_KEY_FIELD = "key";
    public static final String DEFAULT_VALUE_FIELD = "value";

    public String keyField; //key字段
    public String valueField; //value字段

    //构造函数
    public SimpleKeyValueDeserializationSchema() {
        this(DEFAULT_KEY_FIELD,DEFAULT_VALUE_FIELD);
    }
    //构造函数
    public SimpleKeyValueDeserializationSchema(String keyField, String valueField) {
        this.keyField = keyField;
        this.valueField = valueField;
    }

    @Override
    public Map deserializeKeyAndValue(byte[] key, byte[] value) {
        HashMap map = new HashMap(2);
        if (keyField !=null){
            String k = key !=null ? new String(key, StandardCharsets.UTF_8) : null;
            map.put(keyField,k);
        }

        if (valueField !=null){
            String v = value != null ? new String(value,StandardCharsets.UTF_8) : null;
            map.put(valueField,v);
        }

        return map;
    }

    @Override
    public TypeInformation<Map> getProducedType() {
        return null;
    }
}
