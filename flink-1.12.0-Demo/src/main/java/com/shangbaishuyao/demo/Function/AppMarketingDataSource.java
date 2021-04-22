package com.shangbaishuyao.demo.Function;

import com.shangbaishuyao.demo.bean.MarketingUserBehavior;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class AppMarketingDataSource extends RichSourceFunction<MarketingUserBehavior> {
    boolean canRun = true;
    Random random = new Random();
    List<String> channels = Arrays.asList("huawwei", "xiaomi", "apple", "baidu", "qq", "oppo", "vivo");
    List<String> behaviors = Arrays.asList("download", "install", "update", "uninstall");
    @Override
    public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
        while (canRun) {
            MarketingUserBehavior marketingUserBehavior = new MarketingUserBehavior(
                    (long) random.nextInt(1000000),
                    behaviors.get(random.nextInt(behaviors.size())),
                    channels.get(random.nextInt(channels.size())),
                    System.currentTimeMillis());
            ctx.collect(marketingUserBehavior);
            Thread.sleep(1000);
        }
    }
    @Override
    public void cancel() {
        canRun = false;
    }
}
