package www.xubatian.cn.warhouse.bean;

import java.math.BigDecimal;
/*
 * @author: shangbaishuyao
 * @des:
 * @date: 下午12:00 2022/2/16
 **/
public class QueryResult {
    private Integer uid;
    private Integer ad_id;
    private String memberlevel;
    private String register;
    private String appregurl;
    private String regsource;
    private String regsourcename;
    private String adname;
    private String sitename;
    private String vip_level;
    private BigDecimal paymoney;
    private String dt;
    private String dn;

    public Integer getUid() {
        return uid;
    }

    public void setUid(Integer uid) {
        this.uid = uid;
    }

    public Integer getAd_id() {
        return ad_id;
    }

    public void setAd_id(Integer ad_id) {
        this.ad_id = ad_id;
    }

    public String getMemberlevel() {
        return memberlevel;
    }

    public void setMemberlevel(String memberlevel) {
        this.memberlevel = memberlevel;
    }

    public String getRegister() {
        return register;
    }

    public void setRegister(String register) {
        this.register = register;
    }

    public String getAppregurl() {
        return appregurl;
    }

    public void setAppregurl(String appregurl) {
        this.appregurl = appregurl;
    }

    public String getRegsource() {
        return regsource;
    }

    public void setRegsource(String regsource) {
        this.regsource = regsource;
    }

    public String getRegsourcename() {
        return regsourcename;
    }

    public void setRegsourcename(String regsourcename) {
        this.regsourcename = regsourcename;
    }

    public String getAdname() {
        return adname;
    }

    public void setAdname(String adname) {
        this.adname = adname;
    }

    public String getSitename() {
        return sitename;
    }

    public void setSitename(String sitename) {
        this.sitename = sitename;
    }

    public String getVip_level() {
        return vip_level;
    }

    public void setVip_level(String vip_level) {
        this.vip_level = vip_level;
    }

    public BigDecimal getPaymoney() {
        return paymoney;
    }

    public void setPaymoney(BigDecimal paymoney) {
        this.paymoney = paymoney;
    }

    public String getDt() {
        return dt;
    }

    public void setDt(String dt) {
        this.dt = dt;
    }

    public String getDn() {
        return dn;
    }

    public void setDn(String dn) {
        this.dn = dn;
    }
}
