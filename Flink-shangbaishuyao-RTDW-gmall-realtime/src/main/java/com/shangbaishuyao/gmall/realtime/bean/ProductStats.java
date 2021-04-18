package com.shangbaishuyao.gmall.realtime.bean;

/**
 * Author: 上白书妖
 * Date: 2021/2/23
 * @Builder注解
 *      可以使用构造者方式创建对象，给属性赋值
 * @Builder.Default
 *      在使用构造者方式给属性赋值的时候，属性的初始值会丢失
 *      该注解的作用就是修复这个问题
 *      例如：我们在属性上赋值了初始值为0L，如果不加这个注解，通过构造者创建的对象属性值会变为null
 *
 *  DWS层-商品主题宽表的计算
 *  统计主题:商品主题
 *
 *  需求指标	    输出方式     计算来源	        来源层级
 *  点击	    多维分析	    page_log直接可求	dwd
 * 	曝光	    多维分析	    page_log直接可求	dwd
 * 	收藏	    多维分析	    收藏表	        dwd
 * 	加入购物车	多维分析	    购物车表	        dwd
 * 	下单	    可视化大屏	订单宽表	        dwm
 * 	支付	    多维分析	    支付宽表        	dwm
 * 	退款	    多维分析	    退款表	        dwd
 * 	评价	    多维分析	    评价表	        dwd
 */

import lombok.Builder;
import lombok.Data;
import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;

@Data
@Builder //构造者设计模式 其实就是定义一个内部类来去给我外部类属性赋值.
public class ProductStats {
    //------------------------统计的维度-----------------------------
    String stt;//窗口起始时间
    String edt;  //窗口结束时间
    Long sku_id; //sku编号
    String sku_name;//sku名称
    BigDecimal sku_price; //sku单价
    Long spu_id; //spu编号
    String spu_name;//spu名称
    Long tm_id; //品牌编号
    String tm_name;//品牌名称
    Long category3_id;//品类编号
    String category3_name;//品类名称

    //------------------------可度量的值-----------------------------
    @Builder.Default
    Long display_ct = 0L; //曝光数

    @Builder.Default
    Long click_ct = 0L;  //点击数

    @Builder.Default
    Long favor_ct = 0L; //收藏数

    @Builder.Default
    Long cart_ct = 0L;  //添加购物车数

    @Builder.Default
    Long order_sku_num = 0L; //下单商品个数

    @Builder.Default   //下单商品金额
    BigDecimal order_amount = BigDecimal.ZERO;

    @Builder.Default
    Long order_ct = 0L; //订单数

    @Builder.Default   //支付金额
    BigDecimal payment_amount = BigDecimal.ZERO;

    @Builder.Default
    Long paid_order_ct = 0L;  //支付订单数

    @Builder.Default
    Long refund_order_ct = 0L; //退款订单数

    @Builder.Default
    BigDecimal refund_amount = BigDecimal.ZERO;

    @Builder.Default
    Long comment_ct = 0L;//评论数

    @Builder.Default //使用构造者方法给属性赋值,有个问题,属性的初始值会丢失.该注解的作用就是修复这个问题
    Long good_comment_ct = 0L; //好评数

    //可以拿到订单所对应的下单商品的个数.价格.金额等等. 但是我是按照商品来统计的,需要统计当前商品下单了多少次.
    //当前商品在多少个订单中都包含了. 这里存的就是商品所对应的订单.用来统计订单. 有多少个商品包含这个订单
    //统计当前商品对应的订单数.这个set集合的大小,就是商品对应订单的实际数量.
    @Builder.Default
    @TransientSink //无法将set集合保存到Clickhouse里面去,所以我加了一个自定义注解 @TransientSink,表示这个属性是用来辅助计算的.
    Set orderIdSet = new HashSet();  //用于统计订单数

    @Builder.Default
    @TransientSink
    Set paidOrderIdSet = new HashSet(); //用于统计支付订单数

    @Builder.Default
    @TransientSink
    Set refundOrderIdSet = new HashSet();//用于退款支付订单数

    Long ts; //统计时间戳
}
