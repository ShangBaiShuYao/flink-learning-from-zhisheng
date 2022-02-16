package www.xubatian.cn.warehouse.controller

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import www.xubatian.cn.warehouse.service.DwsIcebergService

object DwsIcebergController {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .set("spark.sql.catalog.hadoop_prod", "org.apache.iceberg.spark.SparkCatalog")
      .set("spark.sql.catalog.hadoop_prod.type", "hadoop")
      .set("spark.sql.catalog.hadoop_prod.warehouse", "hdfs://mycluster/spark/warehouse")
      .set("spark.sql.catalog.catalog-name.type", "hadoop")
      .set("spark.sql.catalog.catalog-name.default-namespace", "default")
      .set("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .set("spark.sql.session.timeZone", "GMT+8")
      .setMaster("local[*]")
      .setAppName("dws_app")

    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    DwsIcebergService.getDwsMemberData(sparkSession, "20190722")
  }
}
