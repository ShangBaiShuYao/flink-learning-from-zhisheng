package com.shangbaishuyao.common.model;

/**
 *@Author: 上白书妖
 *@Date: 2020/11/13 11:50
 */
public class UserEvent {
    /**
     * user Id
     */
    private Long id;

    /**
     * User Name
     */
    private String userName;

    /**
     * User email
     */
    private String email;

    /**
     * User phone number
     */
    private String phoneNumber;

    /**
     * User 真实姓名
     */
    private String realName;

    /**
     * User 展示名称
     */
    private String displayName;

    /**
     * User 头像 Url
     */
    private String avatarUrl;

    /**
     * User 密码（加密后）
     */
    private String password;

    /**
     * User 地址
     */
    private String address;

    /**
     * User 注册来源（1：IOS、2：PC、3：Android）
     */
    private int deviceSource;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getPhoneNumber() {
        return phoneNumber;
    }

    public void setPhoneNumber(String phoneNumber) {
        this.phoneNumber = phoneNumber;
    }

    public String getRealName() {
        return realName;
    }

    public void setRealName(String realName) {
        this.realName = realName;
    }

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public String getAvatarUrl() {
        return avatarUrl;
    }

    public void setAvatarUrl(String avatarUrl) {
        this.avatarUrl = avatarUrl;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public int getDeviceSource() {
        return deviceSource;
    }

    public void setDeviceSource(int deviceSource) {
        this.deviceSource = deviceSource;
    }
}
