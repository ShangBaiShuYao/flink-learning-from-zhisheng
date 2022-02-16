package www.xubatian.cn.warhouse.dao;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.FlinkSource;

import static org.apache.flink.table.api.Expressions.$;
/*
 * @author: shangbaishuyao
 * @des:
 * @date: 下午12:01 2022/2/16
 **/
public class DwdIcebergDao {
    public Table getDwdMember(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://mycluster/flink/warehouse/iceberg/dwd_member");
        DataStream<RowData> result = FlinkSource.forRowData().env(env).tableLoader(tableLoader).streaming(false).build();
        Table table = tableEnv.fromDataStream(result);
        return table;
    }

    public Table getDwdPcentermempaymoney(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://mycluster/flink/warehouse/iceberg/dwd_pcentermempaymoney");
        DataStream<RowData> result = FlinkSource.forRowData().env(env).tableLoader(tableLoader).streaming(false).build();
        Table table = tableEnv.fromDataStream(result);
        return table;
    }

    public Table getDwdVipLevel(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://mycluster/flink/warehouse/iceberg/dwd_vip_level");
        DataStream<RowData> result = FlinkSource.forRowData().env(env).tableLoader(tableLoader).streaming(false).build();
        Table table = tableEnv.fromDataStream(result).renameColumns($("start_time").as("vip_start_time"))
                .renameColumns($("end_time").as("vip_end_time")).renameColumns($("last_modify_time").as("vip_last_modify_time"))
                .renameColumns($("max_free").as("vip_max_free")).renameColumns($("min_free").as("vip_min_free"))
                .renameColumns($("next_level").as("vip_next_level")).renameColumns($("operator").as("vip_operator"));
        return table;
    }

    public Table getDwdBaseWebsite(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://mycluster/flink/warehouse/iceberg/dwd_base_website");
        DataStream<RowData> result = FlinkSource.forRowData().env(env).tableLoader(tableLoader).streaming(false).build();
        Table table = tableEnv.fromDataStream(result).renameColumns($("delete").as("site_delete"))
                .renameColumns($("createtime").as("site_createtime")).renameColumns($("creator").as("site_creator"));
        return table;
    }

    public Table getDwdMemberRegtyp(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://mycluster/flink/warehouse/iceberg/dwd_member_regtype");
        DataStream<RowData> result = FlinkSource.forRowData().env(env).tableLoader(tableLoader).streaming(false).build();
        Table table = tableEnv.fromDataStream(result).renameColumns($("createtime").as("reg_createtime"));
        return table;
    }

    public Table getDwdBaseAd(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://mycluster/flink/warehouse/iceberg/dwd_base_ad");
        DataStream<RowData> result = FlinkSource.forRowData().env(env).tableLoader(tableLoader).streaming(false).build();
        Table table = tableEnv.fromDataStream(result);
        return table;

    }
}
