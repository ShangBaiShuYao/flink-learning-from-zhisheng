package com.shangbaishuyao.gmall;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.shangbaishuyao.gmall.mapper")
public class Gmall0820PublisherApplication {

    public static void main(String[] args) {
        SpringApplication.run(Gmall0820PublisherApplication.class, args);
    }

}
