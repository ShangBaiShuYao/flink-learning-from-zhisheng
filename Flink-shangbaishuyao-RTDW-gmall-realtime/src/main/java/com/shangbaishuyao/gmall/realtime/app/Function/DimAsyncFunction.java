package com.shangbaishuyao.gmall.realtime.app.Function;

import com.alibaba.fastjson.JSONObject;
import com.shangbaishuyao.gmall.realtime.utils.DimUtil;
import com.shangbaishuyao.gmall.realtime.utils.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;

/**
 * 优化1：加入旁路缓存模式
 * 优化2：异步查询 <br/>
 * Flink官网案例: https://ci.apache.org/projects/flink/flink-docs-release-1.12/zh/dev/stream/operators/asyncio.html <br/>
 * Author: shangbaishuyao
 * Date: 2021/2/19
 * Desc:  自定义维度异步查询的函数, 泛型通用型,有许多维度关联,支付,订单等等<br/>
 *  模板方法设计模式
 *      在父类中只定义方法的声明，让整个流程跑通
 *      具体的实现延迟到子类中实现
 *
 *
 *  //TODO: 此处为什么是抽象类呢?
 *  因为,我想到维度表里面查询key,这个key是不具体的.也不知道怎么去获取.是去用户维度表查询,还是省份维度表查询等等我都不知道.
 *  如果不知道的话, 获取key的方法就不能定义为具体的方法了.  既然这个方法我也不知道该如何实现. 即,该方法只有声明,没有具体的实现.
 *  那么当前这个方法,就应该定义为抽象方法. 如果一个类中有抽象方法. 那么该类就一定是抽象类.
 *  如果这里创建的是抽闲类,那么在OrderWideApp中初始化DimAsyncFunction抽闲类的时候,就得实现这个类中的抽闲方法.
 *  最后我们都将抽象方法提取到DimJoinFunction(抽象接口)中了.
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T,T> implements DimJoinFunction<T>{
    //线程池对象的父接口生命（多态）
    private ExecutorService executorService;
    //维度的表名
    private String tableName;
    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }
    //初始化
    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化线程池对象
        System.out.println("初始化线程池对象");
        executorService = ThreadPoolUtil.getInstance();
    }
    /**
     * asyncInvoke这个异步操作底层就是通过创建一个线程,在这个线程里面来处理我维度的关联.线程是如何创建的呢? 我使用的是线程池
     * 通过线程池来获取一个线程,并且在run里面干了操作.干了啥呢? 发送异步请求.即①根据维度的key查询维度,②做关联.
     * 这个方法是通过用的. 只要是事实数据和维度数据做关联都可以用这个函数来完成.
     * 但是这里到底怎么获取key,怎么查询维度表,具体怎么做关联.这里是不知道的.所以将这两个方法抽取出来做了一个接口.这接口中的方法是抽象方法.
     * 我具体的实现是在子类中将这个方法重新覆盖写的. 这就是模板方法设计模式.
     * 发送异步请求的方法
     * @param obj     流中的事实数据
     * @param resultFuture      异步处理结束之后，返回结果
     * @throws Exception
     */
    @Override
    public void asyncInvoke(T obj, ResultFuture<T> resultFuture) throws Exception {
        executorService.submit(//表示我现在从线程池中拿一个线程,这个线程要执性什么操作.
            new Runnable() {
                //具体在run方法里面所做的事情就是异步请求操作. 具体是: 根据主键到维度表里面进行查询.
                @Override
                public void run() {
                    try {
                        //发送异步请求. 查询的时候记录一下系统的时间.这个不是必须的.只是我想查询整个花了多长时间.
                        long start = System.currentTimeMillis();
                        //从流中事实数据获取key
                        //TODO 这里也是定义抽象方法
                        String key = getKey(obj);

                        //根据维度的主键到维度表中进行查询. 到那张维度表,查询那个数据,这是需要你传过来的.
                        JSONObject dimInfoJsonObj = DimUtil.getDimInfo(tableName, key);
                        //System.out.println("维度数据Json格式：" + dimInfoJsonObj);

                        if(dimInfoJsonObj != null){
                            //维度关联  流中的事实数据和查询出来的维度数据进行关联
                            //因为这里我不知道是订单的实时数据还是支付的事实数据,也不知道是用户维度还是品类维度
                            //但是我知道需要做关联: (obj:事实数据, dimInfoJsonObj:维度数据)
                            //TODO 这里也是定义抽象方法
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
