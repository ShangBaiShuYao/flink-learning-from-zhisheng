package com.shangbaishuyao.controller;

import com.shangbaishuyao.bean.Top10;
import com.shangbaishuyao.service.PersonService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Desc: 可视化页面展示 <br/>
 * create by shangbaishuyao on 2021/4/1
 * @Author: 上白书妖
 * @Date: 16:27 2021/4/1
 */
@Controller
public class personController {
    @Autowired
    PersonService personService;
    //Top10
    @RequestMapping(value = "/queryActiveTotalData")
    @ResponseBody
    public List<Top10> query(){
        List<Top10> top10s = personService.selectTop10();
//        System.out.println(top10s.toString()+"================");
//        Top10 top101 = new Top10();
        //生成json格式
//        System.out.println(JSON.toJSON(top10));
        //对象转成string
//        String stuString = JSONObject.toJSONString(top10);

//        List list = new ArrayList<>();
//        for (int i = 0; i < top10s.size(); i++){
//            list.add(top10s.get(i).getPid());
//            list.add(top10s.get(i+1).getNumber());
//            top101.setPid(top10s.get(i).getPid());
//            top101.setNumber(top10s.get(i+1).getNumber());
//        }
//        String json = new Gson().toJson(top10s);

//        String s = list.toString();
//        System.out.println(s);
//        JSONObject a = (JSONObject) JSON.toJSON(s);
//        System.out.println(json);
        return top10s;
    }

    @RequestMapping(value = "/queryTOP3")
    @ResponseBody
    public Map<Object,Integer> queryTOP3(){
        //0-15
        int top1 = personService.selectTop1();
        int top2 = personService.selectTop2();
        int top3 = personService.selectTop3();
        Map map = new HashMap<>();
        map.put("top1",top1);
        map.put("top2",top2);
        map.put("top3",top3);
        return map;
    }

}
