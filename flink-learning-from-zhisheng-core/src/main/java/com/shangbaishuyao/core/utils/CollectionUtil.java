/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.shangbaishuyao.core.utils;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.stream.Stream;

/**
 * Desc:  Simple utility to work with Java collections.<br/>
 *        使用Java集合的简单实用程序
 *
 * 知识补充: <br/>
 *      ①Java8 的Stream流 https://www.jianshu.com/p/d3b38ce02709 <br/>
 *
 *@Author: 上白书妖
 *@Date: 2020/11/20 15:23
 */
public class CollectionUtil {
    /**
     *  A safe maximum size for arrays in the JVM.
     *  JVM中数组的安全最大大小
     *
     *  上白书妖补充:
     *  ①源码对list最大大小为什么是Integer.MAX_VALUE - 8?
     *     首先数组头需要存储数组大小信息以及其它的一些信息，假设数组达到了最大则数组大小需要8个字节来存储。
     *     而当留下8个数组大小时则可保证至少有8个字节;存储byte类型时恰好为8个字节，其它类型则大于8个字节）
     *     当然，由于虚拟机的限制完全不可能达到这个大小。
     *     stackOverFlow讨论链接： https://stackoverflow.com/questions/31382531/why-i-cant-create-an-array-with-large-size
     */
    public static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    //AssertionError: 抛出以表明断言失败。
    private CollectionUtil(){
        throw new AssertionError();
    }

    public static boolean isNullOrEmpty(Collection<?> collection){
        return collection == null || collection.isEmpty();
    }

    public static boolean isNullOrEmpty(Map<?,?> map){
        return map == null || map.isEmpty();
    }

    //TODO
    public static <T,R> Stream<R> mapWithIndex(Collection<T> input, final BiFunction<T,Integer,R> mapper){
        AtomicInteger count = new AtomicInteger(0);
        return input.stream().map(element -> mapper.apply(element,count.getAndIncrement()));
    }


    /**
     * Partition a collection into approximately n buckets.
     * 将一个集合划分为大约n个桶
     */
    //TODO
    public static <T> Collection<List<T>> partition(Collection<T> elements , int numBuckets){

        HashMap<Integer, List<T>> buckets = new HashMap<>(numBuckets);

        int initialCapacity = elements.size() / numBuckets;

        int index = 0 ;

        for (T element : elements){
            int bucket = index % numBuckets;
            buckets.computeIfAbsent(bucket,key -> new ArrayList(initialCapacity)).add(element);
        }
        return buckets.values();
    }

}
