package com.shangbaishuyao.gmall.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Desc:
 * Date: 2021/1/29
 * @Component:对当前类进行标记，SpringBoot程序启动之后，会扫描到这个类
 * 将这个类对象的创建以及对象之间关系的维护交给Spring容器进行管理。
 *     >@Controller 控制层
 *     >@Service    业务层
 *     >@Repository 持久层
 * @RequestMapping
 *      接收什么样的请求，交给对应的方法进行处理
 * @如果使用的是Controller注解，那么类中的方法返回值是String类型，那么返回值表示跳转页面的路径
 * @ResponseBody  将返回值以字符串的形式返回给客户端
 *
 * @RestController = @Controller +   @ResponseBody
 * create by shangbaishuyao on 2021/4/9
 * @Author: 上白书妖
 * @Date: 15:47 2021/4/9
 */
@RestController
public class FirstController {
    //处理客户端的请求，并且进行响应
    @RequestMapping("/first")
    public String test(){
        return "this is first";
    }
}

