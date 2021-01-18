package com.shangbaishuyao.sinks.model;

/**
 * Desc: 学生实体类 <br/>
 *
 *@Author: 上白书妖
 *@Date: 2020/11/24 14:56
 */
public class Student {
    private int id;
    private String name;
    private String password;
    private int age;


    public Student(int id, String name, String password, int age) {
        this.id = id;
        this.name = name;
        this.password = password;
        this.age = age;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }
}
