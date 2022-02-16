package www.xubatian.cn.warehouse.dao

import org.apache.spark.sql.SparkSession

object DwDIcebergDao {

  def getDwdMember(sparkSession: SparkSession) = {
    sparkSession.sql("select uid,ad_id ,birthday,email,fullname,iconurl,lastlogin,mailaddr,memberlevel," +
      "password,phone,qq,register,regupdatetime,unitname,userip,zipcode,dt from hadoop_prod.db.dwd_member")
  }

  def getDwdPcentermempaymoney(sparkSession: SparkSession) = {
    sparkSession.sql("select uid,paymoney,siteid,vip_id,dt,dn from hadoop_prod.db.dwd_pcentermempaymoney")
  }

  def getDwdVipLevel(sparkSession: SparkSession) = {
    sparkSession.sql("select vip_id,vip_level,start_time as vip_start_time,end_time as vip_end_time," +
      "last_modify_time as vip_last_modify_time,max_free as vip_max_free,min_free as vip_min_free,next_level as vip_next_level," +
      "operator as vip_operator,dn from hadoop_prod.db.dwd_vip_level")
  }

  def getDwdBaseWebsite(sparkSession: SparkSession) = {
    sparkSession.sql("select siteid,sitename,siteurl,delete as site_delete,createtime as site_createtime,creator as site_creator" +
      ",dn from hadoop_prod.db.dwd_base_website")
  }

  def getDwdMemberRegtyp(sparkSession: SparkSession) = {
    sparkSession.sql("select uid,appkey,appregurl,bdp_uuid,createtime as reg_createtime,isranreg,regsource,regsourcename,websiteid," +
      "dt from hadoop_prod.db.dwd_member_regtype")
  }

  def getDwdBaseAd(sparkSession: SparkSession) = {
    sparkSession.sql("select adid as ad_id,adname,dn from hadoop_prod.db.dwd_base_ad;")
  }

}
