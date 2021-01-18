package com.shangbaishuyao.common.utils;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Calendar;
import java.util.Date;

/**
 * Desc: 日期时间工具类 <br/>
 *
 * 时间工具类里面具体的方法: <br/>
 *    ① 格式化日期        <br/>
 *    ② 根据传入值的时间字符串和格式，输出Date类型 <br/>
 *    ③ 获取当天的开始时间的字符串 <br/>
 *    ④ 获取当天的开始时间的字符串 <br/>
 *    ⑤ 获取当天的结束时间的字符串 <br/>
 *    ⑥ 获取当天的结束时间的字符串 <br/>
 *    ⑦ 获取Now的开始时间的字符串 <br/>
 *       格式默认YYYYMMDDHHMMSS <br/>
 *    ⑧ 获取Now的结束时间的字符串 <br/>
 *       格式默认YYYYMMDDHHMMSS <br/>
 *    ⑨ 根据指定的时间戳获取前 l 或者后 l 天的时间戳 <br/>
 *
 *@Author: 上白书妖
 *@Date: 2020/11/18 16:06
 */
public class DateUtil {

    public static DateTimeFormatter YYYY_MM_DD = DateTimeFormat.forPattern("yyyy-MM-dd");
    public static DateTimeFormatter YYYYMMDD = DateTimeFormat.forPattern("yyyyMMdd");
    public static DateTimeFormatter YYYYMMDDHHMMSS = DateTimeFormat.forPattern("yyyyMMddHHmmss");
    public static DateTimeFormatter YYYY_MM_DD_HH_MM_SS = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
    public static DateTimeFormatter YYYY_MM_DD_HH_MM_SS_SSS = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");
    public static DateTimeFormatter YYYY_MM_DD_HH_MM_SS_SSSZ = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSZ");
    public static DateTimeFormatter YYYY_MM_DD_HH_MM_SS_0 = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.0");
    public static DateTimeFormatter YYYY_MM_DD_HH_MM = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm");
    public static DateTimeFormatter YYYYMMDD_HH_MM_SS = DateTimeFormat.forPattern("yyyy/MM/dd HH:mm:ss");


    public static String format(Date date, DateTimeFormatter dateFormatter) {
        DateTime dateTime = new DateTime(date);
        return dateTime.toString(dateFormatter);
    }

    public static String format(Date date, DateTimeZone timeZone, DateTimeFormatter dateFormatter) {
        DateTime dateTime = new DateTime(date, timeZone);
        return dateTime.toString(dateFormatter);
    }

    public static String format(long timeStamp, DateTimeFormatter dateFormatter) {
        return format(timeStamp, "Asia/Shanghai", dateFormatter);
    }

    public static String format(long timeStamp, String timeZoneId, DateTimeFormatter dateFormatter) {
        DateTimeZone timeZone = DateTimeZone.forID(timeZoneId);
        DateTime dateTime = new DateTime(timeStamp, timeZone);
        return dateTime.toString(dateFormatter);
    }

    /**
     * 格式化日期 <br/>
     *
     * @param time
     * @param dateFormatter
     * @return
     */
    public static long format(String time, DateTimeFormatter dateFormatter) {
        if (YYYY_MM_DD_HH_MM_SS_0.equals(dateFormatter)) {
            return YYYY_MM_DD_HH_MM_SS_0.parseMillis(time);
        } else if (YYYY_MM_DD_HH_MM_SS.equals(dateFormatter)) {
            return YYYY_MM_DD_HH_MM_SS.parseMillis(time);
        } else if (YYYY_MM_DD_HH_MM.equals(dateFormatter)) {
            return YYYY_MM_DD_HH_MM.parseMillis(time);
        } else if (YYYY_MM_DD.equals(dateFormatter)) {
            return YYYY_MM_DD.parseMillis(time);
        } else if (YYYY_MM_DD_HH_MM_SS_SSS.equals(dateFormatter)) {
            return YYYY_MM_DD_HH_MM_SS_SSS.parseMillis(time);
        } else if (YYYY_MM_DD_HH_MM_SS_SSSZ.equals(dateFormatter)) {
            return YYYY_MM_DD_HH_MM_SS_SSSZ.parseMillis(time);
        }
        return YYYY_MM_DD_HH_MM_SS.parseMillis(time);
    }

    public static long format(Date date) {
        return YYYY_MM_DD_HH_MM_SS.parseMillis(format(date, YYYY_MM_DD_HH_MM_SS));
    }


    /**
     * 判断时间是否有效 <br/>
     *
     * @param value
     * @param formatter
     * @return
     */
    public static Boolean isValidDate(String value, DateTimeFormatter formatter) {
        try {
            formatter.parseDateTime(value);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 根据传入值的时间字符串和格式，输出Date类型 <br/>
     *
     * @param value
     * @param formatter
     * @return
     */
    public static Date toDate(String value, DateTimeFormatter formatter) {
        return formatter.parseDateTime(value).toDate();
    }


    /**
     * 获取当天的开始时间的字符串 <br/>
     *
     * @param date 当天日期
     * @return 当天的开始时间
     */
    public static String withTimeAtStartOfDay(Date date, DateTimeFormatter formatter) {
        return new DateTime(date).withTimeAtStartOfDay().toString(formatter);
    }

    /**
     * 获取当天的开始时间的字符串 <br/>
     *
     * @param date 当天日期
     * @return 当天的开始时间
     */
    public static String withTimeAtStartOfDay(DateTime date, DateTimeFormatter formatter) {
        return date.withTimeAtStartOfDay().toString(formatter);
    }

    /**
     * 获取当天的结束时间的字符串 <br/>
     *
     * @param date 当天日期
     * @return 当天的开始时间
     */
    public static String withTimeAtEndOfDay(Date date, DateTimeFormatter formatter) {
        return new DateTime(date).withTimeAtStartOfDay().plusDays(1).minusSeconds(1).toString(formatter);
    }

    /**
     * 获取当天的结束时间的字符串 <br/>
     *
     * @param date 当天日期
     * @return 当天的开始时间
     */
    public static String withTimeAtEndOfDay(DateTime date, DateTimeFormatter formatter) {
        return date.withTimeAtStartOfDay().plusDays(1).minusSeconds(1).toString(formatter);
    }


    /**
     * 获取Now的开始时间的字符串 <br/>
     * 格式默认YYYYMMDDHHMMSS <br/>
     *
     * @return 当天的开始时间
     */
    public static String withTimeAtStartOfNow() {
        return DateTime.now().withTimeAtStartOfDay().toString(YYYYMMDDHHMMSS);
    }

    /**
     * 获取Now的结束时间的字符串 <br/>
     * 格式默认YYYYMMDDHHMMSS <br/>
     *
     * @return 当天的开始时间
     */
    public static String withTimeAtEndOfNow() {
        return DateTime.now().withTimeAtStartOfDay().plusDays(1).minusSeconds(1).toString(YYYYMMDDHHMMSS);
    }


    /**
     * 根据指定的时间戳获取前 l 或者后 l 天的时间戳 <br/>
     *
     * @param timestamp
     * @param l
     * @return
     */
    public static Long getPastTime(Long timestamp, int l) {
        if (timestamp == null) {
            timestamp = System.currentTimeMillis();
        }
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(timestamp);
        calendar.set(Calendar.DATE, calendar.get(Calendar.DATE) + l);
        return format(calendar.getTime());
    }

}
