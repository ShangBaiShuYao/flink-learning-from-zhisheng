package com.shangbaishuyao.gmall.service;

import com.shangbaishuyao.gmall.bean.KeywordStats;

import java.util.List;

/**
 * Author: 上白书妖
 * Date: 2021/3/1
 * Desc: 关键词统计接口
 */
public interface KeywordStatsService {
    public List<KeywordStats> getKeywordStats(int date, int limit);
}

