package com.shangbaishuyao.common.utils;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Assert;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Desc: 测试com.shangbaishuyao.common.utils.DateUtil类 <br/>
 *
 *@Author: 上白书妖
 *@Date: 2020/11/20 13:52
 */
public class DateUtilTests {
    /**
     * junit.framework包下的Assert提供了多个断言方法. 主用于比较测试传递进去的两个参数.
     * Assert.assertEquals();及其重载方法: 1. 如果两者一致, 程序继续往下运行. 2. 如果两者不一致, 中断测试方法, 抛出异常信息 AssertionFailedError .
     */
    @Test
    public void testFormat() {
        Date date1 = new Date(1556665200000L);
        Assert.assertEquals(date1.getTime(), DateUtil.format(date1));

        Assert.assertEquals("2019-05-01", DateUtil.format( date1.getTime(), DateUtil.YYYY_MM_DD));
        Assert.assertEquals("2019-05-01", DateUtil.format(date1, DateTimeZone.getDefault(), DateUtil.YYYY_MM_DD));

        Assert.assertEquals(date1.getTime(), DateUtil.format("2019-05-01", DateUtil.YYYY_MM_DD));
        Assert.assertEquals(1556723220000L, DateUtil.format("2019-05-01 16:07", DateUtil.YYYY_MM_DD_HH_MM));
        Assert.assertEquals(1556723235000L, DateUtil.format("2019-05-01 16:07:15", DateUtil.YYYY_MM_DD_HH_MM_SS));
        Assert.assertEquals(1556723235000L, DateUtil.format("2019-05-01 16:07:15.0", DateUtil.YYYY_MM_DD_HH_MM_SS_0));
    }

    /**
     * assertEquals 和 assertTrue 区别
     * 相同之处：都能判断两个值是否相等
     * assertTrue 如果为true，则运行success，反之Failure
     * assertEquals 如果预期值与真实值相等，则运行success，反之Failure
     *
     * 不同之处：
     * assertEquals 运行Failure会有错误提示，提示预期值是xxx，而实际值是xxx。容易调式
     * assertTrue 没有错误提示
     */
    @Test
    public void testIsValidDate() {
        Assert.assertTrue(DateUtil.isValidDate("2019-05-01", DateUtil.YYYY_MM_DD));
        Assert.assertFalse(DateUtil.isValidDate("01-05-2019", DateUtil.YYYY_MM_DD));
    }

    @Test
    public void testToDate() {
        Assert.assertEquals(new Date(1556665200000L), DateUtil.toDate("2019-05-01", DateUtil.YYYY_MM_DD));
    }


    @Test
    public void testWithTimeAtStartOfDay() {
        DateTimeFormatter dtf =
                DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
        Date date = new Date(1557874800000L);
        DateTime dt = new DateTime(1557874800000L);

        Assert.assertEquals("2019-05-15 00:00:00",
                DateUtil.withTimeAtStartOfDay(date, dtf));
        Assert.assertEquals("2019-05-15 00:00:00",
                DateUtil.withTimeAtStartOfDay(dt, dtf));
    }

    @Test
    public void testWithTimeAtEndOfDay() {
        DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
        Date date = new Date(1557961199000L);
        DateTime dt = new DateTime(1557961199000L);

        Assert.assertEquals("2019-05-15 23:59:59", DateUtil.withTimeAtEndOfDay(date, dtf));
        Assert.assertEquals("2019-05-15 23:59:59", DateUtil.withTimeAtEndOfDay(dt, dtf));
    }

    @Test
    public void testWithTimeAtStartOfNow() {
        Date date = new Date();
             date.setHours(0);
             date.setMinutes(0);
             date.setSeconds(0);
        String data = new SimpleDateFormat("yyyyMMddHHmmss").format(date);
        Assert.assertEquals(data, DateUtil.withTimeAtStartOfNow());
    }

    @Test
    public void testWithTimeAtEndOfNow() {
        Date date = new Date();
             date.setHours(23);
             date.setMinutes(59);
             date.setSeconds(59);
        String data = new SimpleDateFormat("yyyyMMddHHmmss").format(date);
        Assert.assertEquals(data, DateUtil.withTimeAtEndOfNow());
    }

}
