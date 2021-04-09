package com.shangbaishuyao.gmall.realtime;


/**
 * 泛型常见用法
 *  1.泛型模板
 *     >在声明类的时候，在类的后面指定.
 *      如果在类的后面声明了泛型模板，那么在声明和创建类对象的时候，模板中数据类型应该保持一致
 *     >在声明方法的时候，在方法的返回值前面声明
 *
 * 2.泛型通配符
 *     >上限
 *     >下限
 */
class MyNBList<T>{
    //向集合中添加元素
    public void add(T element){
    }
    //从集合获取元素
    //public A get(){
    //}

    public <A>void m1(A e){
        System.out.println(e.getClass());
    }

    //?属于泛型通配符    <? extends Child> 上限
    public void m2(Class<? extends Child> z){
    }

    //?属于泛型通配符    <? extends Child> 下限
    public void m3(Class<? super Child> z){
    }
}

class Parent{}
class Child extends Parent{}
class Sub extends Child{}

public class Test {
    public static void main(String[] args) {
        //泛型的不可变性
        //MyNBList<String> m = new MyNBList<String>();
        //MyNBList<Child> m = new MyNBList<Parent>();
        //MyNBList<Child> m = new MyNBList<Sub>();
        new MyNBList<Parent>().<String>m1("xxxx");
    }
}
