package com.shangbaishuyao.service.impl;

import com.shangbaishuyao.bean.Top10;
import com.shangbaishuyao.model.Person;
import com.shangbaishuyao.service.PersonService;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

@Service
public class PersonServiceImpl implements PersonService {

    @Resource
    private PersonService personMapper;

    @Override
    public int deleteByPrimaryKey(Integer id) {
        return personMapper.deleteByPrimaryKey(id);
    }

    @Override
    public int insert(Person record) {
        return personMapper.insert(record);
    }

    @Override
    public int insertSelective(Person record) {
        return personMapper.insertSelective(record);
    }

    @Override
    public Person selectByPrimaryKey(Integer id) {
        return personMapper.selectByPrimaryKey(id);
    }

    @Override
    public int updateByPrimaryKeySelective(Person record) {
        return personMapper.updateByPrimaryKeySelective(record);
    }

    @Override
    public int updateByPrimaryKey(Person record) {
        return personMapper.updateByPrimaryKey(record);
    }

    @Override
    public List<Top10> selectTop10() {
        List<Top10> top10s = personMapper.selectTop10();
        return top10s;
    }

    @Override
    public int selectTop1() {
        return personMapper.selectTop1();
    }

    @Override
    public int selectTop2() {
        return personMapper.selectTop2();
    }

    @Override
    public int selectTop3() {
        return personMapper.selectTop3();
    }


}
