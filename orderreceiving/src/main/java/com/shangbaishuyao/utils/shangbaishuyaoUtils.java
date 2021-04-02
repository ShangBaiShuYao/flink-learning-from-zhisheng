package com.shangbaishuyao.utils;

import java.util.List;

/**
 * Desc: 上白书妖的工具类 <br/>
 * create by shangbaishuyao on 2021/4/2
 * @Author: 上白书妖
 * @Date: 14:21 2021/4/2
 */
public class shangbaishuyaoUtils {
    /**
     * 将List<Object>转化为JSON数据
     * Object必须有重写toString方法
     * https://blog.csdn.net/zhang_ye_ye/article/details/93906560
     * @param <E>
     */
    public static class ListToJSONUtil<E> {
        private StringBuffer stringBuffer=new StringBuffer();
        public String listTOJSON(List<E> list){

            stringBuffer.append("[");

            for(int i=0;i<list.size();i++){
                E obj=list.get(i);
                String tostr=obj.toString();
                tostr=tostr.substring(tostr.indexOf("{"));
                tostr=tostr.replace("=",":");
                stringBuffer.append(tostr);
                stringBuffer.append(",");
            }
            stringBuffer.replace(stringBuffer.length()-1,stringBuffer.length(),"]");
            return stringBuffer.toString();
        }
    }


}
