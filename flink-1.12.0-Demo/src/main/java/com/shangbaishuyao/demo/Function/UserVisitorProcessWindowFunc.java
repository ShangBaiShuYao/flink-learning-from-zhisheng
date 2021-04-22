package com.shangbaishuyao.demo.Function;

import com.shangbaishuyao.demo.bean.UserBehavior;
import com.shangbaishuyao.demo.bean.UserVisitorCount;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Iterator;

public class UserVisitorProcessWindowFunc extends ProcessWindowFunction<UserBehavior, UserVisitorCount, String, TimeWindow> {
    @Override
    public void process(String s, Context context, Iterable<UserBehavior> elements, Collector<UserVisitorCount> out) throws Exception {
        //创建HashSet用于去重
        HashSet<Long> uids = new HashSet<>();
        //取出窗口中的所有数据
        Iterator<UserBehavior> iterator = elements.iterator();
        //遍历迭代器,将数据中的UID放入HashSet,去重
        while (iterator.hasNext()) {
            uids.add(iterator.next().getUserId());
        }
        //输出数据
        out.collect(new UserVisitorCount("UV",
                new Timestamp(context.window().getEnd()).toString(),
                uids.size()));
    }
}
