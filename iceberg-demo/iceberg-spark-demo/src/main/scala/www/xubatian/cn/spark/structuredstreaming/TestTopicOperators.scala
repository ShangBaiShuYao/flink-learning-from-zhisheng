package www.xubatian.cn.spark.structuredstreaming

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{Dataset, SparkSession}

object TestTopicOperators {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .set("spark.sql.catalog.hadoop_prod", "org.apache.iceberg.spark.SparkCatalog")
      .set("spark.sql.catalog.hadoop_prod.type", "hadoop")
      .set("spark.sql.catalog.hadoop_prod.warehouse", "hdfs://mycluster/spark/warehouse")
      .set("spark.sql.catalog.catalog-name.type", "hadoop")
      .set("spark.sql.catalog.catalog-name.default-namespace", "default")
      .set("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .set("spark.sql.session.timeZone", "GMT+8")
      .set("spark.sql.shuffle.partitions", "12")
      //      .setMaster("local[*]")
      .setAppName("test_topic")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val df = sparkSession.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "hadoop101:9092,hadoop102:9092,hadoop103:9092")
      .option("subscribe", "test2")
      .option("startingOffsets", "earliest")
      .option("maxOffsetsPerTrigger", "20000")
      .load()
    import sparkSession.implicits._


    val query = df.selectExpr("cast (value as string)").as[String]
      .map(item => {
        val array = item.split("\t")
        val uid = array(0)
        val courseid = array(1)
        val deviceid = array(2)
        val ts = array(3)
        Test1(uid.toLong, courseid.toInt, deviceid.toInt, new Timestamp(ts.toLong))
      }).writeStream.foreachBatch { (batchDF: Dataset[Test1], batchid: Long) =>
      batchDF.writeTo("hadoop_prod.db.test_topic").append()
    }.trigger(Trigger.ProcessingTime(1, TimeUnit.MINUTES)).option("checkpointLocation", "/ss/checkpoint")
      .start()
    query.awaitTermination()
  }

  case class Test1(uid: BigInt,
                   courseid: Int,
                   deviceid: Int,
                   ts: Timestamp)

}

