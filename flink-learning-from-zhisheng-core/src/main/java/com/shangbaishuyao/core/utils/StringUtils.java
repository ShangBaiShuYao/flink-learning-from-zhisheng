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

import java.util.Arrays;


/**
 * <br>此工具类编写的源码是: import org.apache.flink.util; 里面的.<br/>
 *
 * Desc:  Utility class to convert objects into strings in vice-versa.(将对象转换为字符串，反之亦然。)
 *
 *@Author: 上白书妖
 *@Date: 2020/11/24 12:13
 */
public class StringUtils {
    //十六进制字符类型
    private static final char[] HEX_CHARS = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

    /**
     * 源码注释: <br/>
     * Given an array of bytes it will convert the bytes to a hex string
     * representation of the bytes.
     *
     * @param bytes
     *        the bytes to convert in a hex string
     * @param start
     *        start index, inclusively
     * @param end
     *        end index, exclusively
     * @return hex string representation of the byte array
     */

    /**
     * ①上白书妖补充: <br/>
     *
     * Desc: 给定一个字节数组，它将把字节转换为十六进制字符串表示的字节。
     *
     * @param bytes 要转换为十六进制字符串的字节
     * @param start 开始指数,包含地 eg: >=
     * @param end 结束索引,排外地,唯一的 eg: >
     * @return 字节数组的十六进制字符串表示
     */
    public static String byteToHexString(final  byte[] bytes , final int start , final int end){
        if (bytes == null){
            //抛出表示一种方法已经通过了非法或不正确的参数。
            throw new IllegalArgumentException("bytes == null");
        }

        int length = end - start;
        char[] out = new char[length * 2];

        for (int i = start , j = 0 ; i < end; i++){
            out[j++] = HEX_CHARS[(0xF0 & bytes[i]) >>> 4];
            out[j++] = HEX_CHARS[0x0F & bytes[i]];
        }
        return new String(out);
    }

    /**
     * ②调用①
     *
     * ②Desc: 这个方法就是调用上面的方法的,将字节数组转换为十六进制字符串表示.
     *         与上面不同的是上面可以自定义开始元素,结束. 但是这里规定字节数组进来必须从头开始转换为string,知道这个数组长度结束,即全部结束之后
     *
     * @param bytes
     * @return
     */
    public static String byteToHexString (final byte[] bytes){
        return byteToHexString(bytes,0,bytes.length);
    }


    /**
     * 源码注释: <br/>
     * Given a hex string this will return the byte array corresponding to the
     * string .
     *
     * @param hex
     *        the hex String array
     * @return a byte array that is a hex string representation of the given
     *         string. The size of the byte array is therefore hex.length/2
     */

    /**
     * 上白书妖补充: <br/>
     *
     * Desc: 给定一个十六进制字符串，它将返回与该字符串对应的字节数组。
     *
     * @param hex 十六进制字符串数组
     * @return 一个字节数组，它是给定字符串的十六进制字符串表示形式。因此字节数组的大小为hex.length/2
     */
    public static byte[] hexStringToByte(final String hex){
        //一个字节数组，它是给定字符串的十六进制字符串表示形式。因此字节数组的大小为hex.length/2
       final byte[] bytes = new byte[hex.length() / 2];
       for (int  i = 0 ; i < bytes.length; i++){
           String substring = hex.substring(2 * i, 2 * i + 2);
           bytes[i] = (byte) Integer.parseInt(substring,16);
       }
       return bytes;
    }

    /**
     * 源码注释: <br/>
     * Returns a string representation of the given array. This method takes an Object to allow also all types of primitive type arrays.
     *
     * @param array The array to create a string representation for.
     * @return The string representation of the array.
     * @throws IllegalArgumentException If the given object is no array.
     */

    /**
     * 上白书妖补充: <br/>
     * 返回给定数组的字符串表示形式。此方法接受一个对象，也允许使用所有类型的基本类型数组。
     *
     * @param array 要为其创建字符串表示形式的数组。
     * @return 数组的字符串表示形式.
     * @throws IllegalArgumentException 如果给定对象不是数组.
     */
    public static String arrayToString(Object array){
        if (array == null){
           throw new NullPointerException();
        }
        //instanceof可以检测某个对象是不是另一个对象的实例
        if (array instanceof int[]){
            return Arrays.toString((int[]) array);
        }
        if (array instanceof long[]) {
            return Arrays.toString((long[]) array);
        }
        if (array instanceof Object[]) {
            return Arrays.toString((Object[]) array);
        }
        if (array instanceof byte[]) {
            return Arrays.toString((byte[]) array);
        }
        if (array instanceof double[]) {
            return Arrays.toString((double[]) array);
        }
        if (array instanceof float[]) {
            return Arrays.toString((float[]) array);
        }
        if (array instanceof boolean[]) {
            return Arrays.toString((boolean[]) array);
        }
        if (array instanceof char[]) {
            return Arrays.toString((char[]) array);
        }
        if (array instanceof short[]) {
            return Arrays.toString((short[]) array);
        }

        //反射得到其类,判断是否是数组
        if (array.getClass().isArray()){
            return "<未知的数组类型> ---英文翻译--- <unknown array type>";
        }else{
            throw new IllegalArgumentException("给定的参数是无数组.  ---英文翻译--- The given argument is no array.");
        }

    }


    /**
     * 源码注释: <br/>
     * This method calls {@link Object#toString()} on the given object, unless the
     * object is an array. In that case, it will use the {@link #arrayToString(Object)}
     * method to create a string representation of the array that includes all contained
     * elements.
     *
     * @param object The object for which to create the string representation.
     * @return The string representation of the object.
     */

    /**
     *上白书妖补充: <br/>
     *  Desc: 此方法在给定对象上调用{@link Object#toString()}，除非该对象是数组。在这种情况下，它将使用{@link #arrayToString(Object)}方法来创建包含所有元素的数组的字符串表示。
     *
     * @param object 要为其创建字符串表示的对象。
     * @return 对象的字符串表示形式。
     */
    public static String arrayAwareToString(Object object){
        if (object == null){
            return null;
        }
        //判断这个{@code Class}对象是否表示数组类。
        if (object.getClass().isArray()){
            //arrayToString(object):方法来创建包含所有元素的数组的字符串表示
            return arrayToString(object);
        }

        return object.toString();
    }


    /**
     * 源码注释: <br/>
     * Replaces control characters by their escape-coded version. For example,
     * if the string contains a line break character ('\n'), this character will
     * be replaced by the two characters backslash '\' and 'n'. As a consequence, the
     * resulting string will not contain any more control characters.
     *
     * @param str The string in which to replace the control characters.
     * @return The string with the replaced characters.
     */

    /**
     * Desc:
     *  用escape编码的版本替换控制字符。例如,
     *  如果字符串包含换行符('\n')，则该字符将
     *  由反斜杠'\'和'n'两个字符替换。因此，
     *  结果字符串将不再包含任何控制字符。
     *
     * @param string 用于替换控制字符的字符串。
     * @return 带有替换字符的字符串。
     */
    public static String showControlCharacters(String string){
        int len = string.length();
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < len; i +=1 ){
            /**
             * 描述
             * java.lang.String.charAt() :方法返回指定索引处的char值。索引范围是从0到length() - 1。对于数组索引，序列的第一个char值是在索引为0，索引1，依此类推，
             * 声明
             * 以下是声明java.lang.String.charAt()方法
             * public char charAt(int index)
             *
             * 参数
             * index -- 这是该指数的char值.
             *
             * 返回值
             * 此方法返回这个字符串的指定索引处的char值。第一个char值的索引为0.
             * 异常
             * IndexOutOfBoundsException -- 如果index参数为负或不小于该字符串的长度.
             */
            char c = string.charAt(i);
            switch (c) {
                case '\b':
                    sb.append("\\b");
                    break;
                case '\t':
                    sb.append("\\t");
                    break;
                case '\n':
                    sb.append("\\n");
                    break;
                case '\f':
                    sb.append("\\f");
                    break;
                case '\r':
                    sb.append("\\r");
                    break;
                default:
                    sb.append(c);
            }
        }
        return sb.toString();
    }


}
