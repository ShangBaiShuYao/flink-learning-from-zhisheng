package com.shangbaishuyao.service;

import com.shangbaishuyao.bean.Top10;
import com.shangbaishuyao.model.Person;

import java.util.List;

/**
 * Desc: PersonService <br/>
 * create by shangbaishuyao on 2021/4/1
 * @Author: 上白书妖
 * @Date: 16:05 2021/4/1
 */
public interface PersonService {
    int deleteByPrimaryKey(Integer id);

    int insert(Person record);

    int insertSelective(Person record);

    Person selectByPrimaryKey(Integer id);

    int updateByPrimaryKeySelective(Person record);

    int updateByPrimaryKey(Person record);

    List<Top10> selectTop10();

    int selectTop1();

    int selectTop2();

    int selectTop3();
}