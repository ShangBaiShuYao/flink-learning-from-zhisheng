package www.xubatian.cn.warhouse.service;

import www.xubatian.cn.warhouse.bean.QueryResult;
import www.xubatian.cn.warhouse.dao.DwsIcbergDao;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;

import static org.apache.flink.table.api.Expressions.$;
/*
 * @author: shangbaishuyao
 * @des:
 * @date: 下午12:01 2022/2/16
 **/
public class AdsIcebergService {
    DwsIcbergDao dwsIcbergDao = new DwsIcbergDao();

    public void queryDetails(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv, String dt) {
        Table table = dwsIcbergDao.queryDwsMemberData(env, tableEnv).where($("dt").isEqual(dt));
        DataStream<QueryResult> queryResultDataStream = tableEnv.toAppendStream(table, QueryResult.class);
        tableEnv.createTemporaryView("tmpA", queryResultDataStream);
        String sql = "select *from(select uid,memberlevel,register,appregurl" +
                ",regsourcename,adname,sitename,vip_level,cast(paymoney as decimal(10,4)),row_number() over" +
                " (partition by memberlevel order by cast(paymoney as decimal(10,4)) desc) as rownum,dn,dt from tmpA where dt='" + dt + "') " +
                " where rownum<4";
        Table table1 = tableEnv.sqlQuery(sql);
        DataStream<RowData> top3DS = tableEnv.toRetractStream(table1, RowData.class).filter(item -> item.f0).map(item -> item.f1);

        String sql2 = "select appregurl,count(uid),dn,dt from tmpA where dt='" + dt + "' group by appregurl,dn,dt";
        Table table2 = tableEnv.sqlQuery(sql2);
        DataStream<RowData> appregurlnumDS = tableEnv.toRetractStream(table2, RowData.class).filter(item -> item.f0).map(item -> item.f1);
        TableLoader top3Table = TableLoader.fromHadoopTable("hdfs://mycluster/flink/warehouse/iceberg/ads_register_top3memberpay");
        TableLoader appregurlnumTable = TableLoader.fromHadoopTable("hdfs://mycluster/flink/warehouse/iceberg/ads_register_appregurlnum");
        FlinkSink.forRowData(top3DS).tableLoader(top3Table).overwrite(true).build();
        FlinkSink.forRowData(appregurlnumDS).tableLoader(appregurlnumTable).overwrite(true).build();

        //Flink SQL> select max(uid),memberlevel,max(paymoney),rownum from iceberg.ads_register_top3memberpay where memberlevel='1'  group by memberlevel,rownum;
    }
}
