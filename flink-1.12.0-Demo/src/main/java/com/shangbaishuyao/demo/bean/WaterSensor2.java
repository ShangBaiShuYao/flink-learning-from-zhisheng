package com.shangbaishuyao.demo.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class WaterSensor2 {
    private String id;
    private Long ts;
    private Double vc;
}
