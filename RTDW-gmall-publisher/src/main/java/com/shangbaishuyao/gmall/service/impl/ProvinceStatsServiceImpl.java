package com.shangbaishuyao.gmall.service.impl;

import com.shangbaishuyao.gmall.bean.ProvinceStats;
import com.shangbaishuyao.gmall.mapper.ProvinceStatsMapper;
import com.shangbaishuyao.gmall.service.ProvinceStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Author: 上白书妖
 * Date: 2021/2/27
 * Desc:按照地区统计的业务接口实现类
 */
@Service
public class ProvinceStatsServiceImpl implements ProvinceStatsService {
    //注入mapper
    @Autowired
    ProvinceStatsMapper provinceStatsMapper;

    @Override
    public List<ProvinceStats> getProvinceStats(int date) {
        return provinceStatsMapper.selectProvinceStats(date);
    }
}
