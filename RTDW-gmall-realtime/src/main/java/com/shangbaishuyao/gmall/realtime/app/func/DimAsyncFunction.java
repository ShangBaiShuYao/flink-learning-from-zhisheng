package com.shangbaishuyao.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.shangbaishuyao.gmall.realtime.utils.DimUtil;
import com.shangbaishuyao.gmall.realtime.utils.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;

/**
 * Author: shangbaishuyao
 * Date: 2021/2/19
 * Desc:  自定义维度异步查询的函数
 *  模板方法设计模式
 *      在父类中只定义方法的声明，让整个流程跑通
 *      具体的实现延迟到子类中实现
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T,T> implements DimJoinFunction<T>{

    //线程池对象的父接口生命（多态）
    private ExecutorService executorService;

    //维度的表名
    private String tableName;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化线程池对象
        System.out.println("初始化线程池对象");
        executorService = ThreadPoolUtil.getInstance();
    }

    /**
     * 发送异步请求的方法
     * @param obj     流中的事实数据
     * @param resultFuture      异步处理结束之后，返回结果
     * @throws Exception
     */
    @Override
    public void asyncInvoke(T obj, ResultFuture<T> resultFuture) throws Exception {
        executorService.submit(
            new Runnable() {
                @Override
                public void run() {
                    try {
                        //发送异步请求
                        long start = System.currentTimeMillis();
                        //从流中事实数据获取key
                        String key = getKey(obj);

                        //根据维度的主键到维度表中进行查询
                        JSONObject dimInfoJsonObj = DimUtil.getDimInfo(tableName, key);
                        //System.out.println("维度数据Json格式：" + dimInfoJsonObj);

                        if(dimInfoJsonObj != null){
                            //维度关联  流中的事实数据和查询出来的维度数据进行关联
                            join(obj,dimInfoJsonObj);
                        }
                        //System.out.println("维度关联后的对象:" + obj);
                        long end = System.currentTimeMillis();
                        System.out.println("异步维度查询耗时" +(end -start)+"毫秒");
                        //将关联后的数据数据继续向下传递
                        resultFuture.complete(Arrays.asList(obj));
                    } catch (Exception e) {
                        e.printStackTrace();
                        throw new RuntimeException(tableName + "维度异步查询失败");
                    }
                }
            }
        );
    }
}
