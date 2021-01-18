package com.shangbaishuyao.common.utils;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import java.nio.charset.Charset;

/**
 * Desc: HTTP 工具类 <br/>
 *
 * 知识补充: <br/>
 *      Apache HttpClient的实例CloseableHttpClient: https://www.jianshu.com/p/99c627c6aa9b  <br/>
 *      HttpClient详细梳理:                         https://www.jianshu.com/p/e86ac9d9ecf0  <br/>
 *      Apache HttpClient的实例CloseableHttpClient: https://www.jianshu.com/p/99c627c6aa9b <br/>
 *
 *@Author: 上白书妖
 *@Date: 2020/11/19 10:12
 */
public class HttpUtil {
    //构造 Basic(基本) Auth(认证方式) 的用户名和密码
    private static final String USER_NAME = "";
    private static final String PASSWORD  = "";

    //GET , POST 等http方法共享
    //Apache HttpClient的实例CloseableHttpClient,使用帮助类HttpClients创建CloseableHttpClient对象.
    private static final CloseableHttpClient httpClient = HttpClients.createDefault();



    /**
     * ①构造Basic Auth认证头信息 <br/>
     *
     * @return authHeader
     */
    private static String getHeader(){
     String auth =USER_NAME +":"+PASSWORD;
     //编码认证
     byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(Charset.forName("UTF-8")));
     //认证头部
     String authHeader = "Basic"+ new String(encodedAuth);
     return authHeader;
    }


    /**
     * ②通过POST 方法发送http请求 <br/>
     *
     * @param url        调用的地址
     * @param jsonParams
     * @return
     * @throws Exception
     */
    public static String doPostString(String url , String jsonParams) throws Exception{
        HttpPost post = new HttpPost(url);
        CloseableHttpResponse response = null;
        String httpString = null;
        try {
            //HttpPost 两种消息体形式 —— UrlEncodedFormEntity 和 StringEntity   https://www.jianshu.com/p/56e640899bc6
            StringEntity stringEntity = new StringEntity(jsonParams, "UTF-8");
            //设置内容的字符编码  单词: Content 内容   encoding 字符编码
            stringEntity.setContentEncoding("UTF-8");
            //设置内容的类型
            stringEntity.setContentType("application/json");

            post.setEntity(stringEntity);
            post.setHeader("content-type", "application/json");
            //如果要设置 Basic Auth 的话
//          httpPost.setHeader("Authorization", getHeader());

            response = httpClient.execute(post);
            httpString = EntityUtils.toString(response.getEntity());

        }catch (Exception e){
            if (response != null){
                EntityUtils.consume(response.getEntity());
                response.close();
            }
        }
        return httpString;
    }


    /**
     * ③通过 GET 方式发起http请求 <br/>
     *
     * @param url 调用的地址
     * @return
     */
    public static String doGetString(String url){
        HttpGet get = new HttpGet();
        get.setHeader("content-type", "application/json");
        //如果要设置 Basic Auth 的话
//      get.setHeader("Authorization", getHeader());
        CloseableHttpResponse response = null;

        try {
            response = httpClient.execute(get);
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK){
                HttpEntity entity = response.getEntity();
                if (entity != null){
                    return EntityUtils.toString(response.getEntity());
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            try {
                if (response!=null){
                    response.close();
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        return null;
    }


    /**
     * ④通过 PUT 方式发起http请求 <br/>
     *
     * @param url         调用的地址
     * @param jsonParams  调用的参数
     * @return
     * @throws Exception
     */
    public static String doPutString(String url, String jsonParams) throws Exception {
        CloseableHttpResponse response = null;
        HttpPut httpPut = new HttpPut(url);
        String httpString;
        try {
            StringEntity entity = new StringEntity(jsonParams, "UTF-8");
            entity.setContentEncoding("UTF-8");
            entity.setContentType("application/json");

            httpPut.setEntity(entity);
            httpPut.setHeader("content-type", "application/json");
            //如果要设置 Basic Auth 的话
//        httpPut.setHeader("Authorization", getHeader());
            response = httpClient.execute(httpPut);
            httpString = EntityUtils.toString(response.getEntity(), "UTF-8");

        } finally {
            if (response != null) {
                EntityUtils.consume(response.getEntity());
                response.close();
            }
        }
        return httpString;
    }


    /**
     * ⑤发送 POST 请求（HTTP），JSON形式 <br/>
     *
     * @param url        调用的地址
     * @param jsonParams 调用的参数
     * @return response
     * @throws Exception
     */
    public static CloseableHttpResponse doPostResponse(String url,String jsonParams) throws Exception{
        HttpPost httpPost = new HttpPost(url);
        CloseableHttpResponse response = null;
        try {
            StringEntity stringEntity = new StringEntity(jsonParams, "UTF-8");
            stringEntity.setContentType("application/json");
            stringEntity.setContentEncoding("UTF-8");

            httpPost.setEntity(stringEntity);
            httpPost.setHeader("content-type","application/json");

            //如果要设置 Basic Auth 的话
            //httpPost.setHeader("Authorization",getHeader());
            response = httpClient.execute(httpPost);
        }finally {
            if (response != null){
                EntityUtils.consume(response.getEntity());
            }
        }
        return response;
    }

}
