package com.shangbaishuyao.gmall;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

//SpringBoot程序执行的入口类
//当SpringBoot程序执行的时候，会扫描同级别包以及子包下的所有标记类，交给Spring进行管理
@SpringBootApplication
public class GmallLoggerApplication {

    public static void main(String[] args) {
        SpringApplication.run(GmallLoggerApplication.class, args);
    }

}
