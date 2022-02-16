package www.xubatian.cn.warehouse.dao

import org.apache.spark.sql.SparkSession

object DwsIcebergDao {
  /**
   * 查询用户宽表数据
   *
   * @param sparkSession
   * @return
   */
  def queryDwsMemberData(sparkSession: SparkSession) = {
    sparkSession.sql("select uid,ad_id,memberlevel,register,appregurl,regsource,regsourcename,adname," +
      "siteid,sitename,vip_level,cast(paymoney as decimal(10,4)) as paymoney,dt,dn from hadoop_prod.db.dws_member ")
  }
}
