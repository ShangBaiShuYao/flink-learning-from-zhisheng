package com.shangbaishuyao.common.utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.lang.reflect.Type;
import java.nio.charset.Charset;

/**
 *Desc: Gson工具类<br/>
 *
 * 知识补充:
 *     disableHtmlEscaping():        https://www.jianshu.com/p/2145c4619f17  <br/>
 *     Gson高级使用和GsonBuilder设置:  https://www.jianshu.com/p/31396863d1aa  <br/>
 *     Java 泛型<T> T 与 T的用法:     https://www.jianshu.com/p/1ea6868efdd1  <br/>
 *
 *@Author: 上白书妖
 *@Date: 2020/11/19 9:33
 */
public class GsonUtil {
     //如果采用new Gson()方式创建Gson则@Expose则没有任何效果，若采用GsonBuilder创建Gson并且调用了excludeFieldsWithoutExposeAnnotation则@Expose将会影响toJson和fromGson序列化和反序列化数据
     private final static Gson gson = new Gson();
     //disableHtmlEscaping():disableHtmlEscaping()中escapeHtmlChars该属性默认为true,表示会将html中的字符例如< >这样的字符处理转义掉。设置为false后，就不会转义这些字符
     private final static Gson disableHtmlEscapingGson = new GsonBuilder().disableHtmlEscaping().create();
     //<T> T表示返回值T是泛型，T只是一个占位符，用来告诉编译器，这个东西先给我留着，等我编译的时候再告诉你是什么类型。
     //单独的T表示限制参数的类型。
     public static <T> T fromJson(String value,Class<T> type){
         return gson.fromJson(value, type);
     }
     //Type是Java编程语言中所有类型的通用超接口。这些类型包括原始类型、参数化类型、数组类    型、类型变量和基本类型。
     public static <T> T fromJson(String value, Type type){
         return gson.fromJson(value,type);
     }
     public static String toJson(Object value){
         return gson.toJson(value);
     }
    //disableHtmlEscaping():disableHtmlEscaping()中escapeHtmlChars该属性默认为true,表示会将html中的字符例如< >这样的字符处理转义掉。设置为false后，就不会转义这些字符
     public static String toJsonDisableHtmlEscaping(Object value){
         return disableHtmlEscapingGson.toJson(value);
     }
     public static byte[] toJSONBytes(Object value){
         return gson.toJson(value).getBytes(Charset.forName("UTF-8"));
     }
}
