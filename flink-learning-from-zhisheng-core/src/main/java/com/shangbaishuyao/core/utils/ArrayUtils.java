/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.shangbaishuyao.core.utils;


/**
 * Desc: Utility class for Java arrays.<br/>
 *       Java数组的实用程序类.<br/>
 *
 *@Author: 上白书妖
 *@Date: 2020/11/20 15:25
 */
public class ArrayUtils {
    public static String[] concat(String[] array1 , String[] array2){
        if (array1.length ==0){
            return array2;
        }
        if (array2.length ==0){
            return array1;
        }

        String[] resultArray = new String[array1.length + array2.length];
        /**
         * 在 Java 编程中经常会遇到数组拷贝操作，一般会有如下四种方式对数组进行拷贝:
         * for遍历，遍历源数组并将每个元素赋给目标数组。
         * clone方法，原数组调用clone方法克隆新对象赋给目标数组。
         * System.arraycopy，JVM 提供的数组拷贝实现。
         * Arrays.copyof，实际也是调用System.arraycopy。
         *
         *
         * @param src      the source array. (数组)
         * @param srcPos   starting position in the source array.(数组中的起始位置)
         * @param dest     the destination array.(拷贝的数据放哪儿.即数组的终点)
         * @param destPos  starting position in the destination data.(指定目标数据中的起始位置,即你可以指定从哪个元素开始)
         * @param length   the number of array elements to be copied.(要复制的数组元素的数目)
         * arrayCopy()本地方法:
         *      public static native void arraycopy(Object src,  int  srcPos,
         *                                          Object dest, int destPos,
         *                                          int length);
         */
        //该方法用于从指定源数组中进行拷贝操作，可以指定开始位置，拷贝指定长度的元素到指定目标数组中。
        System.arraycopy(array1,0,resultArray,0,array1.length);
        System.arraycopy(array2,0,resultArray,array1.length,array2.length);
        return resultArray;
    }
}
