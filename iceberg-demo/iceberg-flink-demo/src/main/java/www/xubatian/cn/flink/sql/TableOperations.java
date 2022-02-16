package www.xubatian.cn.flink.sql;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.flink.source.FlinkSource;
/*
 * @author: shangbaishuyao
 * @des:
 * @date: 下午12:00 2022/2/16
 **/
public class TableOperations {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://mycluster/flink/warehouse/iceberg/testA");

        overtData(env,tableLoader);
        env.execute();
    }

    public static void batchRead(StreamExecutionEnvironment env, TableLoader tableLoader) {
        DataStream<RowData> batch = FlinkSource.forRowData().env(env).tableLoader(tableLoader).streaming(false).build();
        batch.map(item -> item.getInt(0) + "\t" + item.getString(1) + "\t" + item.getInt(2) + "\t" + item.getString(3)).print();
    }

    public static void streamingRead(StreamExecutionEnvironment env, TableLoader tableLoader) {
        DataStream<RowData> stream = FlinkSource.forRowData().env(env).tableLoader(tableLoader).streaming(true).build();
        stream.print();
    }

    public static void appendingData(StreamExecutionEnvironment env,TableLoader tableLoader){
        DataStream<RowData> batch = FlinkSource.forRowData().env(env).tableLoader(tableLoader).streaming(false).build();
        TableLoader tableB = TableLoader.fromHadoopTable("hdfs://mycluster/flink/warehouse/iceberg/testB");
        FlinkSink.forRowData(batch).tableLoader(tableB).build();
    }

    public static void overtData(StreamExecutionEnvironment env,TableLoader tableLoader){
        DataStream<RowData> batch = FlinkSource.forRowData().env(env).tableLoader(tableLoader).streaming(false).build();
        TableLoader tableB = TableLoader.fromHadoopTable("hdfs://mycluster/flink/warehouse/iceberg/testB");
        FlinkSink.forRowData(batch).tableLoader(tableB).overwrite(true).build();
    }
}
