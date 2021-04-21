package com.shangbaishuyao.demo.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PageViewCount {
    private String pv;
    private String time;
    private Integer count;
}
