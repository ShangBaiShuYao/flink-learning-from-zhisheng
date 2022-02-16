package www.xubatian.cn.warehouse.bean

import java.sql.Timestamp

case class BaseWebsite(
                        siteid: Int,
                        sitename: String,
                        siteurl: String,
                        delete: Int,
                        createtime: Timestamp,
                        creator: String,
                        dn: String
                      )

case class MemberRegType(
                          uid: Int,
                          appkey: String,
                          appregurl: String,
                          bdp_uuid: String,
                          createtime: Timestamp,
                          isranreg: String,
                          regsource: String,
                          regsourcename: String,
                          websiteid: Int,
                          dt: String
                        )

case class VipLevel(
                     vip_id: Int,
                     vip_level: String,
                     start_time: Timestamp,
                     end_time: Timestamp,
                     last_modify_time: Timestamp,
                     max_free: String,
                     min_free: String,
                     next_level: String,
                     operator: String,
                     dn: String
                   )


case class DwsMember(
                      uid: Int,
                      ad_id: Int,
                      fullname: String,
                      iconurl: String,
                      lastlogin: String,
                      mailaddr: String,
                      memberlevel: String,
                      password: String,
                      paymoney: String,
                      phone: String,
                      qq: String,
                      register: String,
                      regupdatetime: String,
                      unitname: String,
                      userip: String,
                      zipcode: String,
                      appkey: String,
                      appregurl: String,
                      bdp_uuid: String,
                      reg_createtime: String,
                      isranreg: String,
                      regsource: String,
                      regsourcename: String,
                      adname: String,
                      siteid: String,
                      sitename: String,
                      siteurl: String,
                      site_delete: String,
                      site_createtime: String,
                      site_creator: String,
                      vip_id: String,
                      vip_level: String,
                      vip_start_time: String,
                      vip_end_time: String,
                      vip_last_modify_time: String,
                      vip_max_free: String,
                      vip_min_free: String,
                      vip_next_level: String,
                      vip_operator: String,
                      dt: String,
                      dn: String
                    )


case class DwsMember_Result(
                      uid: Int,
                      ad_id: Int,
                      fullname: String,
                      iconurl: String,
                      lastlogin: String,
                      mailaddr: String,
                      memberlevel: String,
                      password: String,
                      paymoney: String,
                      phone: String,
                      qq: String,
                      register: String,
                      regupdatetime: String,
                      unitname: String,
                      userip: String,
                      zipcode: String,
                      appkey: String,
                      appregurl: String,
                      bdp_uuid: String,
                      reg_createtime: Timestamp,
                      isranreg: String,
                      regsource: String,
                      regsourcename: String,
                      adname: String,
                      siteid: Int,
                      sitename: String,
                      siteurl: String,
                      site_delete: String,
                      site_createtime: String,
                      site_creator: String,
                      vip_id: Int,
                      vip_level: String,
                      vip_start_time: Timestamp,
                      vip_end_time: Timestamp,
                      vip_last_modify_time: Timestamp,
                      vip_max_free: String,
                      vip_min_free: String,
                      vip_next_level: String,
                      vip_operator: String,
                      dt: String,
                      dn: String
                    )

case class QueryResult(
                        uid: Int,
                        ad_id: Int,
                        memberlevel: String,
                        register: String,
                        appregurl: String, //注册来源url
                        regsource: String,
                        regsourcename: String,
                        adname: String,
                        siteid: String,
                        sitename: String,
                        vip_level: String,
                        paymoney: BigDecimal,
                        dt: String,
                        dn: String
                      )