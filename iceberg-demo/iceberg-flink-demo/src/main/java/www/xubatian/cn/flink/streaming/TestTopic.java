package www.xubatian.cn.flink.streaming;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
//flink run -m yarn-cluster -ynm dimetl -p 12 -ys 4 -yjm 1024 -ytm 2048m -d -c www.xubatian.cn.iceberg.flink.streaming.TestTopic  -yqu flink ./icberg-flink-demo-1.0-SNAPSHOT-jar-with-dependencies.jar
/*
 * @author: shangbaishuyao
 * @des:
 * @date: 下午12:00 2022/2/16
 **/
public class TestTopic {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop101:9092,hadoop102:9092,hadoop103:9092");
        properties.setProperty("group.id", "flink-test-group");
        FlinkKafkaConsumer010<String> kafakSource = new FlinkKafkaConsumer010<>("test2", new SimpleStringSchema(), properties);
        kafakSource.setStartFromEarliest();
        DataStream<RowData> result = env.addSource(kafakSource).map(item -> {
            String[] array = item.split("\t");
            Long uid = Long.parseLong(array[0]);
            Integer courseid = Integer.parseInt(array[1]);
            Integer deviceid = Integer.parseInt(array[2]);
            DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            LocalDateTime localDateTime = new Timestamp(Long.parseLong(array[3])).toLocalDateTime();
            String dt = df.format(localDateTime);
            GenericRowData rowData = new GenericRowData(4);
            rowData.setField(0, uid);
            rowData.setField(1, courseid);
            rowData.setField(2, deviceid);
            rowData.setField(3, StringData.fromString(dt));
            return rowData;
        });
        TableLoader testtopicTable = TableLoader.fromHadoopTable("hdfs://mycluster/flink/warehouse/iceberg/test_topic");
        FlinkSink.forRowData(result).tableLoader(testtopicTable).build();
        result.print();
        env.execute();
    }
}
