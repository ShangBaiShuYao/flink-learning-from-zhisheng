package com.shangbaishuyao.sql.ago.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Desc:
 * create by shangbaishuyao on 2021/2/1
 * @Author: 上白书妖
 * @Date: 18:05 2021/2/1
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class wordCount {
    /**
     * word
     */
    public String word;

    /**
     * 出现的次数
     */
    public long c;
}
