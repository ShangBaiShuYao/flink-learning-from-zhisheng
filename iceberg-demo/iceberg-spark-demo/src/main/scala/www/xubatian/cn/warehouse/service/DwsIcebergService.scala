package www.xubatian.cn.warehouse.service

import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.SparkSession
import www.xubatian.cn.warehouse.bean.{DwsMember, DwsMember_Result}
import www.xubatian.cn.warehouse.dao.DwDIcebergDao

/*
 * @author: shangbaishuyao
 * @des:
 * @date: 下午12:06 2022/2/16
 **/
object DwsIcebergService {
  def getDwsMemberData(sparkSession: SparkSession, dt: String) = {
    import sparkSession.implicits._
    val dwdPcentermempaymoney = DwDIcebergDao.getDwdPcentermempaymoney(sparkSession).where($"dt" === dt)
    val dwdVipLevel = DwDIcebergDao.getDwdVipLevel(sparkSession)
    val dwdMember = DwDIcebergDao.getDwdMember(sparkSession).where($"dt" === dt)
    val dwdBaseWebsite = DwDIcebergDao.getDwdBaseWebsite(sparkSession)
    val dwdMemberRegtype = DwDIcebergDao.getDwdMemberRegtyp(sparkSession).where($"dt" === dt)
    val dwdBaseAd = DwDIcebergDao.getDwdBaseAd(sparkSession)
    val result = dwdMember.join(dwdMemberRegtype.drop("dt"), Seq("uid"), "left")
      .join(dwdPcentermempaymoney.drop("dt"), Seq("uid"), "left")
      .join(dwdBaseAd, Seq("ad_id", "dn"), "left")
      .join(dwdBaseWebsite, Seq("siteid", "dn"), "left")
      .join(dwdVipLevel, Seq("vip_id", "dn"), "left_outer")
      .select("uid", "ad_id", "fullname", "iconurl", "lastlogin", "mailaddr", "memberlevel", "password"
        , "paymoney", "phone", "qq", "register", "regupdatetime", "unitname", "userip", "zipcode", "appkey"
        , "appregurl", "bdp_uuid", "reg_createtime", "isranreg", "regsource", "regsourcename", "adname"
        , "siteid", "sitename", "siteurl", "site_delete", "site_createtime", "site_creator", "vip_id", "vip_level",
        "vip_start_time", "vip_end_time", "vip_last_modify_time", "vip_max_free", "vip_min_free", "vip_next_level"
        , "vip_operator", "dt", "dn").as[DwsMember]

    val resultData = result.groupByKey(item => item.uid + "_" + item.dn)
      .mapGroups { case (key, iters) =>
        val keys = key.split("_")
        val uid = Integer.parseInt(keys(0))
        val dn = keys(1)
        val dwsMembers = iters.toList
        val paymoney = dwsMembers.filter(_.paymoney != null).map(item=>BigDecimal.apply(item.paymoney)).reduceOption(_ + _).getOrElse(BigDecimal.apply(0.00)).toString
        val ad_id = dwsMembers.map(_.ad_id).head
        val fullname = dwsMembers.map(_.fullname).head
        val icounurl = dwsMembers.map(_.iconurl).head
        val lastlogin = dwsMembers.map(_.lastlogin).head
        val mailaddr = dwsMembers.map(_.mailaddr).head
        val memberlevel = dwsMembers.map(_.memberlevel).head
        val password = dwsMembers.map(_.password).head
        val phone = dwsMembers.map(_.phone).head
        val qq = dwsMembers.map(_.qq).head
        val register = dwsMembers.map(_.register).head
        val regupdatetime = dwsMembers.map(_.regupdatetime).head
        val unitname = dwsMembers.map(_.unitname).head
        val userip = dwsMembers.map(_.userip).head
        val zipcode = dwsMembers.map(_.zipcode).head
        val appkey = dwsMembers.map(_.appkey).head
        val appregurl = dwsMembers.map(_.appregurl).head
        val bdp_uuid = dwsMembers.map(_.bdp_uuid).head
        val reg_createtime = if (dwsMembers.map(_.reg_createtime).head != null) dwsMembers.map(_.reg_createtime).head else "1970-01-01 00:00:00"
        val isranreg = dwsMembers.map(_.isranreg).head
        val regsource = dwsMembers.map(_.regsource).head
        val regsourcename = dwsMembers.map(_.regsourcename).head
        val adname = dwsMembers.map(_.adname).head
        val siteid = if (dwsMembers.map(_.siteid).head != null) dwsMembers.map(_.siteid).head else "0"
        val sitename = dwsMembers.map(_.sitename).head
        val siteurl = dwsMembers.map(_.siteurl).head
        val site_delete = dwsMembers.map(_.site_delete).head
        val site_createtime = dwsMembers.map(_.site_createtime).head
        val site_creator = dwsMembers.map(_.site_creator).head
        val vip_id = if (dwsMembers.map(_.vip_id).head != null) dwsMembers.map(_.vip_id).head else "0"
        val vip_level = dwsMembers.map(_.vip_level).max
        val vip_start_time = if (dwsMembers.map(_.vip_start_time).min != null) dwsMembers.map(_.vip_start_time).min else "1970-01-01 00:00:00"
        val vip_end_time = if (dwsMembers.map(_.vip_end_time).max != null) dwsMembers.map(_.vip_end_time).max else "1970-01-01 00:00:00"
        val vip_last_modify_time = if (dwsMembers.map(_.vip_last_modify_time).max != null) dwsMembers.map(_.vip_last_modify_time).max else "1970-01-01 00:00:00"
        val vip_max_free = dwsMembers.map(_.vip_max_free).head
        val vip_min_free = dwsMembers.map(_.vip_min_free).head
        val vip_next_level = dwsMembers.map(_.vip_next_level).head
        val vip_operator = dwsMembers.map(_.vip_operator).head
        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
        val reg_createtimeStr = LocalDateTime.parse(reg_createtime, formatter);
        val vip_start_timeStr = LocalDateTime.parse(vip_start_time, formatter)
        val vip_end_timeStr = LocalDateTime.parse(vip_end_time, formatter)
        val vip_last_modify_timeStr = LocalDateTime.parse(vip_last_modify_time, formatter)
        DwsMember_Result(uid, ad_id, fullname, icounurl, lastlogin, mailaddr, memberlevel, password, paymoney,
          phone, qq, register, regupdatetime, unitname, userip, zipcode, appkey, appregurl,
          bdp_uuid, Timestamp.valueOf(reg_createtimeStr), isranreg, regsource, regsourcename, adname, siteid.toInt,
          sitename, siteurl, site_delete, site_createtime, site_creator, vip_id.toInt, vip_level,
          Timestamp.valueOf(vip_start_timeStr), Timestamp.valueOf(vip_end_timeStr), Timestamp.valueOf(vip_last_modify_timeStr), vip_max_free, vip_min_free,
          vip_next_level, vip_operator, dt, dn)
      }
        resultData.write.format("iceberg").mode("overwrite").save("hadoop_prod.db.dws_member")
  }


}
