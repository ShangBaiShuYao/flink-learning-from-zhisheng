package www.xubatian.cn.warehouse.service

import java.sql.Timestamp
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.SparkSession
import www.xubatian.cn.warehouse.bean.{BaseWebsite, MemberRegType, VipLevel}

object DwdIcebergService {

  def readOdsData(sparkSession: SparkSession) = {
    import org.apache.spark.sql.functions._
    import sparkSession.implicits._
    sparkSession.read.json("/ods/baseadlog.log")
      .withColumn("adid", col("adid").cast("Int"))
      .writeTo("hadoop_prod.db.dwd_base_ad").overwritePartitions()

    sparkSession.read.json("/ods/baswewebsite.log").map(item => {
      val createtime = item.getAs[String]("createtime")
      val str = LocalDate.parse(createtime, DateTimeFormatter.ofPattern("yyyy-MM-dd")).atStartOfDay().
        format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
      BaseWebsite(item.getAs[String]("siteid").toInt, item.getAs[String]("sitename"),
        item.getAs[String]("siteurl"), item.getAs[String]("delete").toInt,
        Timestamp.valueOf(str), item.getAs[String]("creator"), item.getAs[String]("dn"))
    }).writeTo("hadoop_prod.db.dwd_base_website").overwritePartitions()

    sparkSession.read.json("/ods/member.log").drop("dn")
      .withColumn("uid", col("uid").cast("int"))
      .withColumn("ad_id", col("ad_id").cast("int"))
      .writeTo("hadoop_prod.db.dwd_member").overwritePartitions()

    sparkSession.read.json("/ods/memberRegtype.log").drop("domain").drop("dn")
      .withColumn("regsourcename", col("regsource"))
      .map(item => {
        val createtime = item.getAs[String]("createtime")
        val str = LocalDate.parse(createtime, DateTimeFormatter.ofPattern("yyyy-MM-dd")).atStartOfDay().
          format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
        MemberRegType(item.getAs[String]("uid").toInt, item.getAs[String]("appkey"),
          item.getAs[String]("appregurl"), item.getAs[String]("bdp_uuid"),
          Timestamp.valueOf(str), item.getAs[String]("isranreg"),
          item.getAs[String]("regsource"), item.getAs[String]("regsourcename"),
          item.getAs[String]("websiteid").toInt, item.getAs[String]("dt"))
      }).writeTo("hadoop_prod.db.dwd_member_regtype").overwritePartitions()

    sparkSession.read.json("/ods/pcenterMemViplevel.log").drop("discountval")
      .map(item => {
        val startTime = item.getAs[String]("start_time")
        val endTime = item.getAs[String]("end_time")
        val last_modify_time = item.getAs[String]("last_modify_time")
        val startTimeStr = LocalDate.parse(startTime, DateTimeFormatter.ofPattern("yyyy-MM-dd")).atStartOfDay().
          format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
        val endTimeStr = LocalDate.parse(endTime, DateTimeFormatter.ofPattern("yyyy-MM-dd")).atStartOfDay().
          format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
        val last_modify_timeStr = LocalDate.parse(last_modify_time, DateTimeFormatter.ofPattern("yyyy-MM-dd")).atStartOfDay().
          format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
        VipLevel(item.getAs[String]("vip_id").toInt, item.getAs[String]("vip_level"),
          Timestamp.valueOf(startTimeStr), Timestamp.valueOf(endTimeStr), Timestamp.valueOf(last_modify_timeStr),
          item.getAs[String]("max_free"), item.getAs[String]("min_free"),
          item.getAs[String]("next_level"), item.getAs[String]("operator"),
          item.getAs[String]("dn"))
      }).writeTo("hadoop_prod.db.dwd_vip_level").overwritePartitions()

    sparkSession.read.json("/ods/pcentermempaymoney.log")
      .withColumn("uid", col("uid").cast("int"))
      .withColumn("siteid", col("siteid").cast("int"))
      .withColumn("vip_id", col("vip_id").cast("int"))
      .writeTo("hadoop_prod.db.dwd_pcentermempaymoney").overwritePartitions()
  }
}
