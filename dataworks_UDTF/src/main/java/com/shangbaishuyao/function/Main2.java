package com.shangbaishuyao.function;

import com.aliyun.odps.udf.UDFException;

/**
 * 测试
 * @author shangbaishuyao
 * @create 2021-09-15 下午8:44
 */

public class Main2 {
    public static void main(String[] args) throws UDFException {
        IPTV6UDTF iptv6UDTF = new IPTV6UDTF();
        Double[] arr = new Double[] {9.2,10.2,12.5,13.5,12.3};
        iptv6UDTF.process(arr);
    }
}
