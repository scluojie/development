package com.flink.day09;

import java.util.Arrays;

public class QuickSortExample {
    public static void main(String[] args) {
        // 2 8 7 1  3  5 6  4
        int[] arr = {3, 7, 4, 9, 5};
        quicksort(arr,0,4);
        System.out.println(Arrays.toString(arr));
    }

    public static void quicksort(int[] arr,int start,int end) {
        //首先判断start 和end 下标大小 作为出递归条件
        if (start > end) {
            return;
        }
        int pivot = partition(arr,start,end);
        quicksort(arr, pivot+1 , end);
        quicksort(arr, start, pivot -1);
    }
    public static int partition(int[] arr, int start,int end){
        //定义哨兵  最后一个元素
        int pivot = arr[end];
        int i = start - 1;
        //  i 3 5  4  9  7   pivot =5
        for (int j = start; j < end; j++) {
            if (arr[j] <= pivot) {
                //前指针后移一位
                i++;
                swap(arr, i, j);
            }
        }
        swap(arr, i + 1, end);
        return i+ 1;
    }


    public static void swap(int[] arr, int i,int j){
        int temp = arr[i];
        arr[i] = arr[j];
        arr[j] = temp;
    }
}
