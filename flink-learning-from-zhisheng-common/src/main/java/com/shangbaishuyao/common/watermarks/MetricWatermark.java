package com.shangbaishuyao.common.watermarks;

import com.shangbaishuyao.common.model.MetricEvent;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * Decs: (Metric)度量(Watermark)水位线 <br/>
 *
 *@Author: 上白书妖
 *@Date: 2020/11/20 12:36
 */
//MetricWatermark: (Metric)度量(Watermark)水位线
//AssignerWithPeriodicWatermarks: (Assigner)指定(Periodic)周期(Watermarks)水印
public class MetricWatermark implements AssignerWithPeriodicWatermarks<MetricEvent> {
    //current(当前) timeStamp(时间戳)
    //一个常数，保持一个long所能拥有的最小值
    private long currentTimestamp = Long.MIN_VALUE;

    /**
     * Desc: 通过对象中得到的时间戳来获取当前时间戳
     *
     * @param metricEvent
     * @param previousElementTimestamp
     * @return
     */
    @Override  //extract(提取) timeStamp(时间戳)            previous(以前的)Element(元素)TimeStamp(时间戳)
    public long extractTimestamp(MetricEvent metricEvent, long previousElementTimestamp) {
        //获取时间戳
        Long timestamp = metricEvent.getTimestamp();
        //Math.max() 比较参数中的最大值。只能传入需要比较的数，不能传入数组。使用apply()方法可以传入数组
        //获取到的时间戳和当前时间戳进行对比,如果 timestamp >= currentTimestamp 则返回 timestamp ,否则返回 currentTimestamp
        currentTimestamp = Math.max(timestamp, currentTimestamp);
        return timestamp;
    }

    /**
     * Desc: 通过当前时间戳(currentTimeStamp)得到当前的水位线(WaterMark)
     *
     *  @Nullable:
     * 在写程序的时候你可以定义是否可为空指针。
     * 通过使用像@NotNull和@Nullable之类的annotation(注释)来声明一个方法是否是空指针安全的。
     * 现代的编译器、IDE或者工具可以读此annotation并帮你添加忘记的空指针检查，或者向你提示出不必要的乱七八糟的空指针检查。
     * IntelliJ和findbugs已经支持了这些annotation。这些annotation同样是JSR 305的一部分，但即便IDE或工具中没有，这个annotation本身可以作为文档。
     * 看到@NotNull和@Nullable，程序员自己可以决定是否做空指针检查。顺便说一句，这个技巧对Java程序员来说相对比较新，要采用需要一段时间。
     *
     * @return watermark 水位线
     */
    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        //单词: Lag 延迟
        //最大延迟时间
        long maxTimeLag = 0L;
        //创建具有给定时间戳(以毫秒为单位)的新水位线。
        Watermark watermark = new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - maxTimeLag);
        return watermark;
    }

}
