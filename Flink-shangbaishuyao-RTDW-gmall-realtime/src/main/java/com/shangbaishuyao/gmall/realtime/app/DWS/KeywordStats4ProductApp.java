package com.shangbaishuyao.gmall.realtime.app.DWS;

import com.shangbaishuyao.gmall.realtime.app.Function.KeywordUDTF;
import com.shangbaishuyao.gmall.realtime.bean.KeywordStats;
import com.shangbaishuyao.gmall.realtime.utils.ClickHouseUtil;
import com.shangbaishuyao.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Author: shangbaishuyao
 * Date: 2021/3/1
 * Desc:  从商品统计中获取关键词
 */
public class KeywordStats4ProductApp {
    public static void main(String[] args) throws Exception {
        //TODO 0.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(4);
        /*
        //CK相关设置
        env.enableCheckpointing(5000, CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        StateBackend fsStateBackend = new FsStateBackend(
                "hdfs://hadoop202:8020/gmall/flink/checkpoint/ProvinceStatsSqlApp");
        env.setStateBackend(fsStateBackend);
        System.setProperty("HADOOP_USER_NAME","shangbaishuyao");
        */
        //TODO 1.定义Table流环境
        EnvironmentSettings settings = EnvironmentSettings
            .newInstance()
            .inStreamingMode()
            .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        //TODO 2.注册自定义函数
        tableEnv.createTemporarySystemFunction("ik_analyze",  KeywordUDTF.class);
        tableEnv.createTemporarySystemFunction("keywordProductC2R",  KeywordProductC2RUDTF.class);

        //TODO 3.将数据源定义为动态表
        String groupId = "keyword_stats_app";
        String productStatsSourceTopic ="dws_product_stats";

        tableEnv.executeSql("CREATE TABLE product_stats (spu_name STRING, " +
            "click_ct BIGINT," +
            "cart_ct BIGINT," +
            "order_ct BIGINT ," +
            "stt STRING,edt STRING ) " +
            "  WITH ("+ MyKafkaUtil.getKafkaDDL(productStatsSourceTopic,groupId)+")");

        //TODO 6.聚合计数
        Table keywordStatsProduct = tableEnv.sqlQuery("select keyword,ct,source, " +
            "DATE_FORMAT(stt,'yyyy-MM-dd HH:mm:ss')  stt," +
            "DATE_FORMAT(edt,'yyyy-MM-dd HH:mm:ss') as edt, " +
            "UNIX_TIMESTAMP()*1000 ts from product_stats  , " +
            "LATERAL TABLE(ik_analyze(spu_name)) as T(keyword) ," +
            "LATERAL TABLE(keywordProductC2R( click_ct ,cart_ct,order_ct)) as T2(ct,source)");

        //TODO 7.转换为数据流
        DataStream<KeywordStats> keywordStatsProductDataStream =
            tableEnv.<KeywordStats>toAppendStream(keywordStatsProduct, KeywordStats.class);

        keywordStatsProductDataStream.print();
        //TODO 8.写入到ClickHouse
        keywordStatsProductDataStream.addSink(
            ClickHouseUtil.<KeywordStats>getJdbcSink(
                "insert into keyword_stats(keyword,ct,source,stt,edt,ts) values(?,?,?,?,?,?)"
            )
        );
        env.execute();
    }
}
