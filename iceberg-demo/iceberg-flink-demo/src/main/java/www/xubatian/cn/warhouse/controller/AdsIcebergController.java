package www.xubatian.cn.warhouse.controller;

import www.xubatian.cn.warhouse.service.AdsIcebergService;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
/*
 * @author: shangbaishuyao
 * @des:
 * @date: 下午12:00 2022/2/16
 **/
public class AdsIcebergController {
    public static AdsIcebergService adsIcebergService=new AdsIcebergService();
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        adsIcebergService.queryDetails(env,tableEnv,"20190722");
        env.execute();
    }
}
