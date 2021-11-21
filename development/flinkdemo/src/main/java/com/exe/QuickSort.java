package com.exe;

import java.util.Arrays;
import java.util.Iterator;

public class QuickSort {

    public static void main(String[] args) {
        int[] arr = {7, 3, 5, 9, 1, 6,10,3,111};
        int[] sorted = quickSort(arr, 0, arr.length - 1);
        Arrays.stream(sorted).mapToObj(s-> {return s + " ";}).forEach(System.out::print);
       // System.out.println(Arrays.toString(sorted));

    }

    public static int[] quickSort(int[] arr,int begin,int end){
        if(begin >= end){
            return arr;
        }
        //定义一个临时变量 保存 中间元素值
        int temp = arr[begin];
        int i = begin;
        int j = end;
        while(i != j){
            for (; j > i && arr[j]>=temp ; ) {

                --j;
            }
            int swap = arr[j];
            arr[j] = arr[i];
            arr[i] = swap;


            for (;i< j && arr[i]<=temp;){
                //交换两值

                ++i;
            }
            int swap2 = arr[j];
            arr[j] = arr[i];
            arr[i] = swap2;
        }
        quickSort(arr,begin,i-1);
        quickSort(arr,i+1,end);

        return arr;
    }

}
