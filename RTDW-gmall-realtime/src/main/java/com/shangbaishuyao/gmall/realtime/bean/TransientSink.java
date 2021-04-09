package com.shangbaishuyao.gmall.realtime.bean;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Author: Felix
 * Date: 2021/2/23
 * Desc: 用该注解标记的属性，不需要插入到ClickHouse
 */
@Target(FIELD)
@Retention(RUNTIME)
public @interface TransientSink {
}
