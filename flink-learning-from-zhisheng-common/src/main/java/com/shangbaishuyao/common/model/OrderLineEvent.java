package com.shangbaishuyao.common.model;

import java.util.List;

/**
 *Desc: 挂号线事件<br/>
 *
 *@Author: 上白书妖
 *@Date: 2020/11/13 11:46
 */
public class OrderLineEvent {
    /**
     * 订单行 Id
     */
    private Long orderLineId;

    /**
     * 购物单 Id
     */
    private Long purchaseOrderId;

    /**
     * 店铺订单 Id
     */
    private Long orderId;

    /**
     * 买家 Id
     */
    private Long buyerId;

    /**
     * 买家 name
     */
    private String buyerName;

    /**
     * 店铺 Id
     */
    private String shopId;

    /**
     * 店铺 name
     */
    private String shopName;

    /**
     * 支付状态（0：未支付、1：已支付）
     */
    private int payStatus;

    /**
     * 支付完成时间（timestamp）
     */
    private Long payAt;

    /**
     * 发货状态（0：未发货、1：待发货、2：已发货）
     */
    private int deliveryStatus;

    /**
     * 签收状态（0：未签收、1：已签收）
     */
    private int receiveStatus;

    /**
     * 退货状态（0：未退货、1：已退货）
     */
    private int reverseStatus;

    /**
     * 发货时间（timestamp）
     */
    private Long shippingAt;

    /**
     * 确认收货时间（timestamp）
     */
    private Long confirmAt;

    /**
     * 买家留言
     */
    private String buyerNotes;

    /**
     * 订单来源（1：IOS、2：PC、3：Android）
     */
    private int deviceSource;

    /**
     * 商品 name
     */
    private String name;

    /**
     * 商品 id
     */
    private Long id;

    /**
     * 商品标签
     */
    private List<String> tags;

    /**
     * 购买数量
     */
    private int count;

    /**
     * 实际支付金额
     */
    private Long payAmount;

    public Long getOrderLineId() {
        return orderLineId;
    }

    public void setOrderLineId(Long orderLineId) {
        this.orderLineId = orderLineId;
    }

    public Long getPurchaseOrderId() {
        return purchaseOrderId;
    }

    public void setPurchaseOrderId(Long purchaseOrderId) {
        this.purchaseOrderId = purchaseOrderId;
    }

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public Long getBuyerId() {
        return buyerId;
    }

    public void setBuyerId(Long buyerId) {
        this.buyerId = buyerId;
    }

    public String getBuyerName() {
        return buyerName;
    }

    public void setBuyerName(String buyerName) {
        this.buyerName = buyerName;
    }

    public String getShopId() {
        return shopId;
    }

    public void setShopId(String shopId) {
        this.shopId = shopId;
    }

    public String getShopName() {
        return shopName;
    }

    public void setShopName(String shopName) {
        this.shopName = shopName;
    }

    public int getPayStatus() {
        return payStatus;
    }

    public void setPayStatus(int payStatus) {
        this.payStatus = payStatus;
    }

    public Long getPayAt() {
        return payAt;
    }

    public void setPayAt(Long payAt) {
        this.payAt = payAt;
    }

    public int getDeliveryStatus() {
        return deliveryStatus;
    }

    public void setDeliveryStatus(int deliveryStatus) {
        this.deliveryStatus = deliveryStatus;
    }

    public int getReceiveStatus() {
        return receiveStatus;
    }

    public void setReceiveStatus(int receiveStatus) {
        this.receiveStatus = receiveStatus;
    }

    public int getReverseStatus() {
        return reverseStatus;
    }

    public void setReverseStatus(int reverseStatus) {
        this.reverseStatus = reverseStatus;
    }

    public Long getShippingAt() {
        return shippingAt;
    }

    public void setShippingAt(Long shippingAt) {
        this.shippingAt = shippingAt;
    }

    public Long getConfirmAt() {
        return confirmAt;
    }

    public void setConfirmAt(Long confirmAt) {
        this.confirmAt = confirmAt;
    }

    public String getBuyerNotes() {
        return buyerNotes;
    }

    public void setBuyerNotes(String buyerNotes) {
        this.buyerNotes = buyerNotes;
    }

    public int getDeviceSource() {
        return deviceSource;
    }

    public void setDeviceSource(int deviceSource) {
        this.deviceSource = deviceSource;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public List<String> getTags() {
        return tags;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public Long getPayAmount() {
        return payAmount;
    }

    public void setPayAmount(Long payAmount) {
        this.payAmount = payAmount;
    }
}
