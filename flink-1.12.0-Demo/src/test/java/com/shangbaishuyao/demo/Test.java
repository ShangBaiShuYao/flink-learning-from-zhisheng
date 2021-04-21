package com.shangbaishuyao.demo;

import java.text.ParseException;
import java.text.SimpleDateFormat;
/**
 * Author: shangbaishuyao
 * Date: 0:08 2021/4/22
 * Desc:
 */
public class Test {
    public static void main(String[] args) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println(sdf.parse("2017-11-26 11:00:00.0"));
    }
}
