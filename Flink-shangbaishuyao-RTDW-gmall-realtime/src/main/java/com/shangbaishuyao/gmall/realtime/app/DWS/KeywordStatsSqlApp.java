package com.shangbaishuyao.gmall.realtime.app.DWS;

import com.shangbaishuyao.gmall.realtime.app.Function.KeywordUDTF;
import com.shangbaishuyao.gmall.realtime.bean.KeywordStats;
import com.shangbaishuyao.gmall.realtime.common.GmallConstant;
import com.shangbaishuyao.gmall.realtime.utils.ClickHouseUtil;
import com.shangbaishuyao.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Author: shangbaishuyao
 * Date: 2021/2/26
 * Desc: dWS层 搜索关键字
 */
public class KeywordStatsSqlApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1 创建Flink流式处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);
        /*
        //1.3 检查点CK相关设置
        env.enableCheckpointing(5000, CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        StateBackend fsStateBackend = new FsStateBackend(
                "hdfs://hadoop202:8020/gmall/flink/checkpoint/ProductStatsApp");
        env.setStateBackend(fsStateBackend);
        System.setProperty("HADOOP_USER_NAME","shangbaishuyao");
        */
        //1.4 创建Table环境
        EnvironmentSettings setting = EnvironmentSettings
            .newInstance()
            .inStreamingMode()
            .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, setting);

        //TODO 2.注册自定义函数
        tableEnv.createTemporarySystemFunction("ik_analyze", KeywordUDTF.class);

        //TODO 3.创建动态表
        //3.1 声明主题以及消费者组
        String pageViewSourceTopic = "dwd_page_log";
        String groupId = "keywordstats_app_group";
        //3.2建表  ts: 1613957384809
        //如图片: 关键字搜索
        tableEnv.executeSql(
     "CREATE TABLE page_view (" +
                " common MAP<STRING, STRING>," +
                " page MAP<STRING, STRING>," +
                " ts BIGINT," +  //指定为事件时间字段  FROM_UNIXTIME: 将距离1970-01-01 00:00:00的秒 转换为指定格式字符串 , TO_TIMESTAMP:将字符串日期转换为Timestamp
                " rowtime as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000,'yyyy-MM-dd HH:mm:ss'))," +
                " WATERMARK FOR rowtime AS rowtime - INTERVAL '2' SECOND) " +
                " WITH (" + MyKafkaUtil.getKafkaDDL(pageViewSourceTopic, groupId) + ")"
        );
        //TODO 4.从动态表中查询数据  这是查询还没有分词前的 全量的table  结果: 上白书妖真帅 ----> [上 ,白 , 书, 妖, 真帅]
        Table fullwordTable = tableEnv.sqlQuery(
            "select page['item'] fullword,rowtime " +
                " from page_view " +
                " where page['page_id']='good_list' and page['item'] IS NOT NULL"
        );
        //用上面查询的结果:  上白书妖真帅 和表函数,  [上 ,白 , 书, 妖, 真帅] 一次进行关联
        //TODO 5.利用自定义函数  对搜索关键词进行拆分
        Table keywordTable = tableEnv.sqlQuery(                                     //函数                 取别名
            "SELECT keyword, rowtime FROM  " + fullwordTable + "," + "LATERAL TABLE(ik_analyze(fullword)) AS t(keyword)"
        );
        //查询
        //TODO 6.分组、开窗、聚合
        Table reduceTable = tableEnv.sqlQuery(
                //统计关键词数量                关键词类型 :表示搜索关键词
            "select keyword,count(*) ct,  '" + GmallConstant.KEYWORD_SEARCH + "' source," +
                "DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt," + //窗口开始时间
                "DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt ," + //窗口结束时间
                "UNIX_TIMESTAMP()*1000 ts from " + keywordTable +
                "group by TUMBLE(rowtime, INTERVAL '10' SECOND),keyword"  //开窗怎么开的通过group by
        );
        //TODO 7.转换为流
        DataStream<KeywordStats> keywordStatsDS = tableEnv.toAppendStream(reduceTable, KeywordStats.class);

        //TODO 8.写入到ClickHouse
        keywordStatsDS.addSink(
            ClickHouseUtil.getJdbcSink("insert into keyword_stats(keyword,ct,source,stt,edt,ts) values(?,?,?,?,?,?)")
        );
        env.execute();
    }
}
