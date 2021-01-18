package com.shangbaishuyao.common.model;
/**
 *@Author: 上白书妖
 *@Date: 2020/11/13 11:49
 */
public class ShopEvent {
    /**
     * Shop Id
     */
    private Long id;

    /**
     * Shop name
     */
    private String name;

    /**
     * shop owner Id
     */
    private Long ownerId;

    /**
     * shop owner name
     */
    private String ownerName;

    /**
     * shop status: (1:正常, -1:关闭, -2:冻结)
     */
    private int status;

    /**
     * shop type: (1:门店 2:商家 3:出版社)
     */
    private int type;

    /**
     * shop phone
     */
    private String phone;

    /**
     * shop email
     */
    private String email;

    /**
     * shop address
     */
    private String address;

    /**
     * shop image url
     */
    private String imageUrl;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getOwnerId() {
        return ownerId;
    }

    public void setOwnerId(Long ownerId) {
        this.ownerId = ownerId;
    }

    public String getOwnerName() {
        return ownerName;
    }

    public void setOwnerName(String ownerName) {
        this.ownerName = ownerName;
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

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getImageUrl() {
        return imageUrl;
    }

    public void setImageUrl(String imageUrl) {
        this.imageUrl = imageUrl;
    }
}
