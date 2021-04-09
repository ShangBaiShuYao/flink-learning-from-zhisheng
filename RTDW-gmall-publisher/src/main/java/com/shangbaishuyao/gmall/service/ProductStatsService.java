package com.shangbaishuyao.gmall.service;

import com.shangbaishuyao.gmall.bean.ProductStats;

import java.math.BigDecimal;
import java.util.List;

/**
 * Author: Felix
 * Date: 2021/2/26
 * Desc: 商品统计service接口
 */
public interface ProductStatsService {
    //获取某一天交易总额
    BigDecimal getGMV(int date);

    //获取某一天不同品牌的交易额
    List<ProductStats> getProductStatsByTrademark(int date,int limit);

    //获取某一天不同品类的交易额
    List<ProductStats> getProductStatsByCategory3(int date,int limit);

    //获取某一天不同SPU的交易额
    List<ProductStats> getProductStatsBySPU(int date,int limit);
}
