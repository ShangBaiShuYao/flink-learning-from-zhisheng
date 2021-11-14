package com.shangbaishuyao.Utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;

/**
 * @author shangbaishuyao
 * @create 2021-11-14 下午1:43
 */

public class JSONUtils {
    public static boolean isJSONValidate(String log){
        try {
            JSON.parse(log);
            return true;
        }catch (JSONException e){
            return false;
        }
    }
}