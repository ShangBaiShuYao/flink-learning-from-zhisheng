package com.shangbaishuyao.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
public class HelloContrller {
    @RequestMapping("/hello")
    public String hello() {
        System.out.println("进入后台");
        return "hello";
    }
    @RequestMapping("/index")
    public String index() {
        System.out.println("进入后台");
        return "index";
    }
}