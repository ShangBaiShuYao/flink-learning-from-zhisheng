package com.shangbaishuyao.demo.Function;

import com.shangbaishuyao.demo.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

//自定义周期性的Watermark生成器
public class MyPunt implements WatermarkGenerator<WaterSensor> {
    private Long maxTs;
    private Long maxDelay;

    public MyPunt(Long maxDelay) {
        this.maxDelay = maxDelay;
        this.maxTs = Long.MIN_VALUE + maxDelay + 1;
    }
    //当数据来的时候调用
    @Override
    public void onEvent(WaterSensor event, long eventTimestamp, WatermarkOutput output) {
        System.out.println("取数据中最大的时间戳");
        maxTs = Math.max(eventTimestamp, maxTs);
        output.emitWatermark(new Watermark(maxTs - maxDelay));
    }
    //周期性调用
    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
    }
}