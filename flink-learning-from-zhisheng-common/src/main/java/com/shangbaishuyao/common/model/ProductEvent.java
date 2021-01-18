package com.shangbaishuyao.common.model;

import java.util.List;

/**
 *@Author: 上白书妖
 *@Date: 2020/11/13 11:48
 */
public class ProductEvent {
    /**
     * Product Id
     */
    private Long id;

    /**
     * Product 类目 Id
     */
    private Long categoryId;

    /**
     * Product 编码
     */
    private String code;

    /**
     * Product 店铺 Id
     */
    private Long shopId;

    /**
     * Product 店铺 name
     */
    private String shopName;

    /**
     * Product 品牌 Id
     */
    private Long brandId;

    /**
     * Product 品牌 name
     */
    private String brandName;

    /**
     * Product name
     */
    private String name;

    /**
     * Product 图片地址
     */
    private String imageUrl;

    /**
     * Product 状态（1(上架),-1(下架),-2(冻结),-3(删除)）
     */
    private int status;

    /**
     * Product 类型
     */
    private int type;

    /**
     * Product 标签
     */
    private List<String> tags;

    /**
     * Product 价格（以分为单位）
     */
    private Long price;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(Long categoryId) {
        this.categoryId = categoryId;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public Long getShopId() {
        return shopId;
    }

    public void setShopId(Long shopId) {
        this.shopId = shopId;
    }

    public String getShopName() {
        return shopName;
    }

    public void setShopName(String shopName) {
        this.shopName = shopName;
    }

    public Long getBrandId() {
        return brandId;
    }

    public void setBrandId(Long brandId) {
        this.brandId = brandId;
    }

    public String getBrandName() {
        return brandName;
    }

    public void setBrandName(String brandName) {
        this.brandName = brandName;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getImageUrl() {
        return imageUrl;
    }

    public void setImageUrl(String imageUrl) {
        this.imageUrl = imageUrl;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public List<String> getTags() {
        return tags;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }

    public Long getPrice() {
        return price;
    }

    public void setPrice(Long price) {
        this.price = price;
    }
}
