package www.xubatian.cn.warhouse.service;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
/*
 * @author: shangbaishuyao
 * @des:
 * @date: 下午12:01 2022/2/16
 **/
public class DwdIcebergSerivce {
    public void readOdsData(StreamExecutionEnvironment env) {

        DataStream<String> baseadDS = env.readTextFile("hdfs://mycluster/ods/baseadlog.log");
        DataStream<RowData> baseadInput = baseadDS.map(item -> {
            JSONObject jsonObject = JSONObject.parseObject(item);
            GenericRowData rowData = new GenericRowData(3);
            rowData.setField(0, jsonObject.getIntValue("adid"));
            rowData.setField(1, StringData.fromString(jsonObject.getString("adname")));
            rowData.setField(2, StringData.fromString(jsonObject.getString("dn")));
            return rowData;
        });
        TableLoader dwdbaseadTable = TableLoader.fromHadoopTable("hdfs://mycluster/flink/warehouse/iceberg/dwd_base_ad");
        FlinkSink.forRowData(baseadInput).tableLoader(dwdbaseadTable).overwrite(true).build();


        DataStream<String> basewebsiteDS = env.readTextFile("hdfs://mycluster/ods/baswewebsite.log");
        DataStream<RowData> basewebsiteInput = basewebsiteDS.map(item -> {
            JSONObject jsonObject = JSONObject.parseObject(item);
            GenericRowData rowData = new GenericRowData(7);
            rowData.setField(0, jsonObject.getIntValue("siteid"));
            rowData.setField(1, StringData.fromString(jsonObject.getString("sitename")));
            rowData.setField(2, StringData.fromString(jsonObject.getString("siteurl")));
            rowData.setField(3, jsonObject.getIntValue("delete"));
            LocalDateTime localDateTime = LocalDate.parse(jsonObject.getString("createtime"), DateTimeFormatter.ofPattern("yyyy-MM-dd"))
                    .atStartOfDay();
            rowData.setField(4, TimestampData.fromLocalDateTime(localDateTime));
            rowData.setField(5, StringData.fromString(jsonObject.getString("creator")));
            rowData.setField(6, StringData.fromString(jsonObject.getString("dn")));
            return rowData;
        });
        TableLoader dwdbasewebsiteTable = TableLoader.fromHadoopTable("hdfs://mycluster/flink/warehouse/iceberg/dwd_base_website");
        FlinkSink.forRowData(basewebsiteInput).tableLoader(dwdbasewebsiteTable).overwrite(true).build();


        DataStream<String> memberDS = env.readTextFile("hdfs://mycluster/ods/member.log");
        DataStream<RowData> memberInput = memberDS.map(item -> {
            JSONObject jsonObject = JSONObject.parseObject(item);
            GenericRowData rowData = new GenericRowData(19);
            rowData.setField(0, jsonObject.getIntValue("uid"));
            rowData.setField(1, jsonObject.getIntValue("ad_id"));
            rowData.setField(2, StringData.fromString(jsonObject.getString("birthday")));
            rowData.setField(3, StringData.fromString(jsonObject.getString("email")));
            rowData.setField(4, StringData.fromString(jsonObject.getString("fullname")));
            rowData.setField(5, StringData.fromString(jsonObject.getString("iconurl")));
            rowData.setField(6, StringData.fromString(jsonObject.getString("lastlogin")));
            rowData.setField(7, StringData.fromString(jsonObject.getString("mailaddr")));
            rowData.setField(8, StringData.fromString(jsonObject.getString("memberlevel")));
            rowData.setField(9, StringData.fromString(jsonObject.getString("password")));
            rowData.setField(10, StringData.fromString(jsonObject.getString("paymoney")));
            rowData.setField(11, StringData.fromString(jsonObject.getString("phone")));
            rowData.setField(12, StringData.fromString(jsonObject.getString("qq")));
            rowData.setField(13, StringData.fromString(jsonObject.getString("register")));
            rowData.setField(14, StringData.fromString(jsonObject.getString("regupdatetime")));
            rowData.setField(15, StringData.fromString(jsonObject.getString("unitname")));
            rowData.setField(16, StringData.fromString(jsonObject.getString("userip")));
            rowData.setField(17, StringData.fromString(jsonObject.getString("zipcode")));
            rowData.setField(18, StringData.fromString(jsonObject.getString("dt")));
            return rowData;
        });
        TableLoader dwdmemberTable = TableLoader.fromHadoopTable("hdfs://mycluster/flink/warehouse/iceberg/dwd_member");
        FlinkSink.forRowData(memberInput).tableLoader(dwdmemberTable).overwrite(true).build();


        DataStream<String> memberregtypDS = env.readTextFile("hdfs://mycluster/ods/memberRegtype.log");
        DataStream<RowData> memberregtypInput = memberregtypDS.map(item -> {
            JSONObject jsonObject = JSONObject.parseObject(item);
            GenericRowData rowData = new GenericRowData(10);
            rowData.setField(0, jsonObject.getIntValue("uid"));
            rowData.setField(1, StringData.fromString(jsonObject.getString("appkey")));
            rowData.setField(2, StringData.fromString(jsonObject.getString("appregurl")));
            rowData.setField(3, StringData.fromString(jsonObject.getString("bdp_uuid")));
            LocalDateTime localDateTime = LocalDate.parse(jsonObject.getString("createtime"), DateTimeFormatter.ofPattern("yyyy-MM-dd"))
                    .atStartOfDay();
            rowData.setField(4, TimestampData.fromLocalDateTime(localDateTime));
            rowData.setField(5, StringData.fromString(jsonObject.getString("isranreg")));
            rowData.setField(6, StringData.fromString(jsonObject.getString("regsource")));
            rowData.setField(7, StringData.fromString(jsonObject.getString("regsource")));
            rowData.setField(8, jsonObject.getIntValue("websiteid"));
            rowData.setField(9, StringData.fromString(jsonObject.getString("dt")));
            return rowData;
        });
        TableLoader dwdmemberregtypeTable = TableLoader.fromHadoopTable("hdfs://mycluster/flink/warehouse/iceberg/dwd_member_regtype");
        FlinkSink.forRowData(memberregtypInput).tableLoader(dwdmemberregtypeTable).overwrite(true).build();

        DataStream<String> memviplevelDS = env.readTextFile("hdfs://mycluster/ods/pcenterMemViplevel.log");
        DataStream<RowData> memviplevelInput = memviplevelDS.map(item -> {
            JSONObject jsonObject = JSONObject.parseObject(item);
            GenericRowData rowData = new GenericRowData(10);
            rowData.setField(0, jsonObject.getIntValue("vip_id"));
            rowData.setField(1, StringData.fromString(jsonObject.getString("vip_level")));
            LocalDateTime start_timeDate = LocalDate.parse(jsonObject.getString("start_time"), DateTimeFormatter.ofPattern("yyyy-MM-dd"))
                    .atStartOfDay();
            LocalDateTime end_timeDate = LocalDate.parse(jsonObject.getString("end_time"), DateTimeFormatter.ofPattern("yyyy-MM-dd"))
                    .atStartOfDay();
            LocalDateTime last_modify_timeDate = LocalDate.parse(jsonObject.getString("last_modify_time"), DateTimeFormatter.ofPattern("yyyy-MM-dd"))
                    .atStartOfDay();
            rowData.setField(2, TimestampData.fromLocalDateTime(start_timeDate));
            rowData.setField(3, TimestampData.fromLocalDateTime(end_timeDate));
            rowData.setField(4, TimestampData.fromLocalDateTime(last_modify_timeDate));
            rowData.setField(5, StringData.fromString(jsonObject.getString("max_free")));
            rowData.setField(6, StringData.fromString(jsonObject.getString("min_free")));
            rowData.setField(7, StringData.fromString(jsonObject.getString("next_level")));
            rowData.setField(8, StringData.fromString(jsonObject.getString("operator")));
            rowData.setField(9, StringData.fromString(jsonObject.getString("dn")));
            return rowData;
        });
        TableLoader dwdviplevelTable = TableLoader.fromHadoopTable("hdfs://mycluster/flink/warehouse/iceberg/dwd_vip_level");
        FlinkSink.forRowData(memviplevelInput).tableLoader(dwdviplevelTable).overwrite(true).build();

        DataStream<String> mempaymoneyDS = env.readTextFile("hdfs://mycluster/ods/pcentermempaymoney.log");
        DataStream<RowData> mempaymoneyInput = mempaymoneyDS.map(item -> {
            JSONObject jsonObject = JSONObject.parseObject(item);
            GenericRowData rowData = new GenericRowData(6);
            rowData.setField(0, jsonObject.getIntValue("uid"));
            rowData.setField(1, StringData.fromString(jsonObject.getString("paymoney")));
            rowData.setField(2, jsonObject.getIntValue("siteid"));
            rowData.setField(3, jsonObject.getIntValue("vip_id"));
            rowData.setField(4, StringData.fromString(jsonObject.getString("dt")));
            rowData.setField(5, StringData.fromString(jsonObject.getString("dn")));
            return rowData;
        });
        TableLoader dwdmempaymoneyTable = TableLoader.fromHadoopTable("hdfs://mycluster/flink/warehouse/iceberg/dwd_pcentermempaymoney");
        FlinkSink.forRowData(mempaymoneyInput).tableLoader(dwdmempaymoneyTable).overwrite(true).build();


    }
}
