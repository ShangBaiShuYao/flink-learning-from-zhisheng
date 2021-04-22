package com.shangbaishuyao.demo.Function;

import com.shangbaishuyao.demo.bean.WaterSensor;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class MyRedisMapper implements RedisMapper<WaterSensor> {
    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.HSET, "Sensor");
    }
    @Override
    public String getKeyFromData(WaterSensor data) {
        return data.getId();
    }

    @Override
    public String getValueFromData(WaterSensor data) {
        return data.getVc().toString();
    }
}
