package com.shangbaishuyao.sources.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Desc: 学生对象实体类<br/>
 * create by shangbaishuyao 2020-12-11
 *@Author: 上白书妖
 *@Date: 2020/12/11 13:55
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Student {
    public int id;
    public String name;
    public String password;
    public int age;
}
