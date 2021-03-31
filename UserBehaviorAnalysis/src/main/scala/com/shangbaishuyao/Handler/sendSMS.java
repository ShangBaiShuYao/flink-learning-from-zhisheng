package com.shangbaishuyao.Handler;

import com.aliyuncs.CommonRequest;
import com.aliyuncs.CommonResponse;
import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.IAcsClient;
import com.aliyuncs.exceptions.ClientException;
import com.aliyuncs.exceptions.ServerException;
import com.aliyuncs.http.MethodType;
import com.aliyuncs.profile.DefaultProfile;
import com.google.gson.Gson;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.Map;

/**
 * Desc:
 * LTAI4FwU2Fd4xz1mbaQJBUgB
 * xPneF1UWkvN6Q9qlpveaV9zbw2FJzN
 * create by shangbaishuyao on 2021/3/31
 * @Author: 上白书妖
 * @Date: 16:12 2021/3/31
 */
public class sendSMS {
    /**
     * 发送短信
     * @param PhoneNumbers
     * @param param
     * @return
     */
    public static void send(String PhoneNumbers, Map<String,Object> param) {

        DefaultProfile profile = DefaultProfile.getProfile(
                "default",
                "LTAI4FwU2Fd4xz1mbaQJBUgB",
                "xPneF1UWkvN6Q9qlpveaV9zbw2FJzN");
        IAcsClient client = new DefaultAcsClient(profile);

        CommonRequest request = new CommonRequest();
        //request.setProtocol(ProtocolType.HTTPS);
        request.setMethod(MethodType.POST);
        request.setDomain("dysmsapi.aliyuncs.com");
        request.setVersion("2017-05-25");
        request.setAction("SendSms");

        request.putQueryParameter("PhoneNumbers", PhoneNumbers);
        request.putQueryParameter("SignName", "上白塔");
        request.putQueryParameter("TemplateCode", "SMS_179602326");
        request.putQueryParameter("TemplateParam", JSONObject.valueToString(param));

        try {
            CommonResponse response = client.getCommonResponse(request);

            Gson gson = new Gson();
            HashMap<String, Object> map = gson.fromJson(response.getData(), HashMap.class);
            String message = (String)map.get("Message");
            String code = (String)map.get("Code");
            if(!"OK".equals(code)){
                System.out.println("短信发送失败");
            }
        } catch (ServerException e) {
            e.printStackTrace();
        } catch (ClientException e) {
            e.printStackTrace();
        }
    }

    public static void send2(String PhoneNumbers, Map<String,Object> param) {
        DefaultProfile profile = DefaultProfile.getProfile("default", "LTAI4FwU2Fd4xz1mbaQJBUgB", "xPneF1UWkvN6Q9qlpveaV9zbw2FJzN");
        IAcsClient client = new DefaultAcsClient(profile);

        CommonRequest request = new CommonRequest();
        request.setMethod(MethodType.POST);
        request.setDomain("dysmsapi.aliyuncs.com");
        request.setVersion("2017-05-25");
        request.setAction("SendSms");
        request.putQueryParameter("PhoneNumbers", "19956571280");
        request.putQueryParameter("SignName", "上白塔");
        request.putQueryParameter("TemplateCode", "SMS_179602326");
        request.putQueryParameter("TemplateParam", "{\"code\":\"12345\"}");
        request.putQueryParameter("SmsUpExtendCode", "");
        try {
            CommonResponse response = client.getCommonResponse(request);
            System.out.println(response.getData());
        } catch (ServerException e) {
            e.printStackTrace();
        } catch (ClientException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) throws Exception{
        send2("19956571280",null);
    }
}
