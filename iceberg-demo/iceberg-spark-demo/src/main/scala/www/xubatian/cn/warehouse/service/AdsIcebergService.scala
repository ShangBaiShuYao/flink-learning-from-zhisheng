package www.xubatian.cn.warehouse.service

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{SaveMode, SparkSession}
import www.xubatian.cn.warehouse.bean.QueryResult
import www.xubatian.cn.warehouse.dao.DwsIcebergDao

object AdsIcebergService {
  def queryDetails(sparkSession: SparkSession, dt: String) = {
    import sparkSession.implicits._
    val result = DwsIcebergDao.queryDwsMemberData(sparkSession).as[QueryResult].where(s"dt='${dt}'")
    result.cache()

    //统计根据url统计人数  wordcount
    result.mapPartitions(partition => {
      partition.map(item => (item.appregurl + "_" + item.dn + "_" + item.dt, 1))
    }).groupByKey(_._1)
      .mapValues(item => item._2).reduceGroups(_ + _)
      .map(item => {
        val keys = item._1.split("_")
        val appregurl = keys(0)
        val dn = keys(1)
        val dt = keys(2)
        (appregurl, item._2, dt, dn)
      }).toDF("appregurl", "num", "dt", "dn").writeTo("hadoop_prod.db.ads_register_appregurlnum").overwritePartitions()

    //统计各memberlevel等级 支付金额前三的用户
    import org.apache.spark.sql.functions._
    result.withColumn("rownum", row_number().over(Window.partitionBy("memberlevel").orderBy(desc("paymoney"))))
      .where("rownum<4").orderBy("memberlevel", "rownum")
      .select("uid", "memberlevel", "register", "appregurl", "regsourcename", "adname"
        , "sitename", "vip_level", "paymoney", "rownum", "dt", "dn")
      .writeTo("hadoop_prod.db.ads_register_top3memberpay").overwritePartitions()
  }
}
