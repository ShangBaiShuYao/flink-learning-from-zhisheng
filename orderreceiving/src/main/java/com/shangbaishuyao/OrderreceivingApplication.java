package com.shangbaishuyao;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(value = "com.shangbaishuyao.*")
public class OrderreceivingApplication {
    public static void main(String[] args) {
        SpringApplication.run(OrderreceivingApplication.class, args);
        System.out.println("可视化展示启动完成");
    }
}
