package www.xubatian.cn.warhouse.service;

import www.xubatian.cn.warhouse.dao.DwdIcebergDao;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
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
public class DwsIcebergService {
    DwdIcebergDao dwdIcebergDao;

    public void getDwsMemberData(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv, String dt) {
        dwdIcebergDao = new DwdIcebergDao();
        Table dwdPcentermempaymoney = dwdIcebergDao.getDwdPcentermempaymoney(env, tableEnv).where($("dt").isEqual(dt));
        Table dwdVipLevel = dwdIcebergDao.getDwdVipLevel(env, tableEnv);
        Table dwdMember = dwdIcebergDao.getDwdMember(env, tableEnv).where($("dt").isEqual(dt));
        Table dwdBaseWebsite = dwdIcebergDao.getDwdBaseWebsite(env, tableEnv);
        Table dwdMemberRegtype = dwdIcebergDao.getDwdMemberRegtyp(env, tableEnv).where($("dt").isEqual(dt));
        Table dwdBaseAd = dwdIcebergDao.getDwdBaseAd(env, tableEnv);

        Table result = dwdMember.dropColumns($("paymoney")).leftOuterJoin(dwdMemberRegtype.renameColumns($("uid").as("reg_uid")).dropColumns($("dt")),
                $("uid").isEqual($("reg_uid")))
                .leftOuterJoin(dwdPcentermempaymoney.renameColumns($("uid").as("pcen_uid")).dropColumns($("dt")),
                        $("uid").isEqual($("pcen_uid")))
                .leftOuterJoin(dwdBaseAd.renameColumns($("dn").as("basead_dn")),
                        $("ad_id").isEqual($("adid")).and($("dn").isEqual($("basead_dn"))))
                .leftOuterJoin(dwdBaseWebsite.renameColumns($("siteid").as("web_siteid")).renameColumns($("dn").as("web_dn")),
                        $("siteid").isEqual($("web_siteid")).and($("dn").isEqual("web_dn")))
                .leftOuterJoin(dwdVipLevel.renameColumns($("vip_id").as("v_vip_id")).renameColumns($("dn").as("vip_dn")),
                        $("vip_id").isEqual($("v_vip_id")).and($("dn").isEqual($("vip_dn"))))
                .groupBy($("uid"))
                .select($("uid"), $("ad_id").min(), $("fullname").min(), $("iconurl").min(), $("lastlogin").min(), $("mailaddr").min(), $("memberlevel").min(), $("password").min()
                        , $("paymoney").cast(DataTypes.DECIMAL(10, 4)).sum().cast(DataTypes.STRING()), $("phone").min(), $("qq").min(), $("register").min(), $("regupdatetime").min(), $("unitname").min(), $("userip").min(), $("zipcode").min(), $("appkey").min()
                        , $("appregurl").min(), $("bdp_uuid").min(), $("reg_createtime").min().cast(DataTypes.STRING()), $("isranreg").min(), $("regsource").min(), $("regsourcename").min(), $("adname").min()
                        , $("siteid").min(), $("sitename").min(), $("siteurl").min(), $("site_delete").min(), $("site_createtime").min(), $("site_creator").min(), $("vip_id").min(), $("vip_level").min(),
                        $("vip_start_time").min().cast(DataTypes.STRING()), $("vip_end_time").max().cast(DataTypes.STRING()), $("vip_last_modify_time").max().cast(DataTypes.STRING()), $("vip_max_free").min(), $("vip_min_free").min(), $("vip_next_level").min()
                        , $("vip_operator").min(), $("dt").min(), $("dn").min());

        DataStream<Tuple2<Boolean, RowData>> tuple2DataStream = tableEnv.toRetractStream(result, RowData.class);
        DataStream<RowData> resultDs = tuple2DataStream.filter(item -> item.f0).map(item -> item.f1);

        TableLoader dwsmember = TableLoader.fromHadoopTable("hdfs://mycluster/flink/warehouse/iceberg/dws_member");
        FlinkSink.forRowData(resultDs).tableLoader(dwsmember).overwrite(true).build();

    }

}
