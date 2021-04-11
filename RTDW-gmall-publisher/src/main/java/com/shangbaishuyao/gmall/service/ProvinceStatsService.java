package com.shangbaishuyao.gmall.service;

import com.shangbaishuyao.gmall.bean.ProvinceStats;

import java.util.List;

/**
 * Author: 上白书妖
 * Date: 2021/2/27
 * Desc:  按照地区统计的业务接口
 */
public interface ProvinceStatsService {
    List<ProvinceStats> getProvinceStats(int date);
}
