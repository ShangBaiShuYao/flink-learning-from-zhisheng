package com.shangbaishuyao.gmall.service.impl;

import com.shangbaishuyao.gmall.bean.KeywordStats;
import com.shangbaishuyao.gmall.mapper.KeywordStatsMapper;
import com.shangbaishuyao.gmall.service.KeywordStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Author: 上白书妖
 * Date: 2021/3/1
 * Desc:关键词统计接口实现类
 */
@Service
public class KeywordStatsServiceImpl implements KeywordStatsService {

    @Autowired
    KeywordStatsMapper keywordStatsMapper;

    @Override
    public List<KeywordStats> getKeywordStats(int date, int limit) {
        return keywordStatsMapper.selectKeywordStats(date,limit);
    }
}
