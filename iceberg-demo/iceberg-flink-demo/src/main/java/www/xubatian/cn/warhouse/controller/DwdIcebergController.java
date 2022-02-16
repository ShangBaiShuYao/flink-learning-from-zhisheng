package www.xubatian.cn.warhouse.controller;

import www.xubatian.cn.warhouse.service.DwdIcebergSerivce;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
/*
 * @author: shangbaishuyao
 * @des:
 * @date: 下午12:00 2022/2/16
 **/
public class DwdIcebergController {
    public static DwdIcebergSerivce dwdIcebergSerivce;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        dwdIcebergSerivce = new DwdIcebergSerivce();
        dwdIcebergSerivce.readOdsData(env);
        env.execute();
    }
}
