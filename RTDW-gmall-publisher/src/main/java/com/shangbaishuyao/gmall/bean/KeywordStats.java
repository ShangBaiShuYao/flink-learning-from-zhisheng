package com.shangbaishuyao.gmall.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Author: 上白书妖
 * Date: 2021/3/1
 * Desc: 关键词统计实体类
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class KeywordStats {
    private String stt;
    private String edt;
    private String keyword;
    private Long ct;
    private String ts;
}

