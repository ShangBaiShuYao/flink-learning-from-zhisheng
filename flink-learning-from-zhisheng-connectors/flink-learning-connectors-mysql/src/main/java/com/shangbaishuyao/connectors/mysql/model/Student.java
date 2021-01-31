package com.shangbaishuyao.connectors.mysql.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/*
 * Desc:
 * create by shangbaishuyao on 2021/1/31
 * @Author: 上白书妖
 * @Date: 14:51 2021/1/31
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Student {
    public int id;
    public String name;
    public String password;
    public int age;
}

