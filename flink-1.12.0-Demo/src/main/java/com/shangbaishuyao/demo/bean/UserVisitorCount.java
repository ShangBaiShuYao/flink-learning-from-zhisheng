package com.shangbaishuyao.demo.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserVisitorCount {
    private String uv;
    private String time;
    private Integer count;
}
