package com.shangbaishuyao.demo.Function;

import com.shangbaishuyao.demo.bean.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SplitProcessFunc extends ProcessFunction<WaterSensor, WaterSensor> {
    @Override
    public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
        //取出水位线
        Integer vc = value.getVc();
        //根据水位线高低,分流
        if (vc >= 30) {
            //将数据输出至主流
            out.collect(value);
        } else {
            //将数据输出至侧输出流
            ctx.output(new OutputTag<Tuple2<String, Integer>>("SideOut") {}, new Tuple2<>(value.getId(), vc));
        }
    }
}
