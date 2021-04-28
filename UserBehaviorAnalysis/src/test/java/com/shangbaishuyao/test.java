package com.shangbaishuyao;

import java.util.Arrays;
//冒泡排序
public class test {
    public static void main(String[] args) throws Exception{
        int[] arr = new int[]{2,5,1,3,6};
        //控制多少轮
        for (int i = 1; i < arr.length; i++){
            //控制每轮次数
            for (int j = 0; j <= arr.length-1-i; j++){
                if (arr[j] > arr[j+1]){
                    int temp;
                    temp = arr[j];
                    arr[j] = arr[j+1];
                    arr[j+1] = temp;
                }
            }
        }
        System.out.println(Arrays.toString(arr));
    }
}
