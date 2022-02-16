package www.xubatian.cn.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object TableOperations {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .set("spark.sql.catalog.hadoop_prod", "org.apache.iceberg.spark.SparkCatalog")
      .set("spark.sql.catalog.hadoop_prod.type", "hadoop")
      .set("spark.sql.catalog.hadoop_prod.warehouse", "hdfs://mycluster/spark/warehouse")
      .set("spark.sql.catalog.catalog-name.type", "hadoop")
      .set("spark.sql.catalog.catalog-name.default-namespace", "db")
      .set("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .set("spark.sql.session.timeZone", "GMT+8")
      .setMaster("local[*]").setAppName("table_operations")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    OverWriteTable2(sparkSession)
  }

  /**
   * 读取iceberg的表
   * @param sparkSession
   */
  def readTale(sparkSession: SparkSession) = {
    //三种方式
    sparkSession.table("hadoop_prod.db.test1").show()
    //    sparkSession.read.format("iceberg").load("hadoop_prod.db.testA").show()
    //    sparkSession.read.format("iceberg").load("/spark/warehouse/db/testA").show() //路径到表就行，不要到具体文件
  }

  def readSnapShots(sparkSession: SparkSession) = {
    //根据查询 hadoop_prod.db.testA.snapshots快照表可以知道快照时间和快照id
    //根据时间戳读取，必须是时间戳 不能使用格式化后的时间
    //    sparkSession.read
    //      .option("as-of-timestamp", "1625465264000") //毫秒时间戳，查询比该值时间更早的快照
    //      .format("iceberg")
    //      .load("hadoop_prod.db.testA").show()

    //根据快照id查询
    sparkSession.read
      .option("snapshot-id", "2103006813793355242")
      .format("iceberg")
      .load("hadoop_prod.db.testA").show()
  }

  case class Student(id: Int, name: String, age: Int, dt: String)

  def writeAndCreateTable(sparkSession: SparkSession) = {
    import sparkSession.implicits._
    import org.apache.spark.sql.functions._
    val data = sparkSession.createDataset[Student](Array(Student(1001, "张三", 18, "2021-06-28"),
      Student(1002, "李四", 19, "2021-06-29"), Student(1003, "王五", 20, "2021-06-29")))
    data.writeTo("hadoop_prod.db.test1").partitionedBy(col("dt")) //指定dt为分区列
      .create()
  }


  def AppendTable(sparkSession: SparkSession) = {
    //两种方式
    import sparkSession.implicits._
    val data = sparkSession.createDataset(Array(Student(1003, "王五", 11, "2021-06-29"), Student(1004, "赵六", 10, "2021-06-30")))
    data.writeTo("hadoop_prod.db.test1").append() //使用DataFrameWriterV2 API
//    data.write.format("iceberg").mode("append").save("hadoop_prod.db.test1") //使用DataFrameWriterV1 API

  }

  /**
   * 动态覆盖
   * @param sparkSession
   */
  def OverWriteTable(sparkSession: SparkSession) = {
    import sparkSession.implicits._
    val data = sparkSession.createDataset(Array(Student(1003, "王五1", 11, "2021-06-29"), Student(1004, "赵六1", 10, "2021-06-30")))
    data.writeTo("hadoop_prod.db.test1").overwritePartitions() //动态覆盖,只会刷新所属分区数据
  }


  def OverWriteTable2(sparkSession: SparkSession) = {
    import sparkSession.implicits._
    val data = sparkSession.createDataset(Array(Student(1, "s1", 1, "111"), Student(2, "s2", 2, "111")))
    data.writeTo("hadoop_prod.db.test1").overwrite($"dt" === "2021-06-30")
  }

}

