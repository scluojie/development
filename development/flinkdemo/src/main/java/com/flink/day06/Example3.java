package com.flink.day06;

import java.util.HashMap;

//n个台阶 每次只能爬1 个台阶  2个台阶
public class Example3 {
    public static void main(String[] args) {
//        System.out.println(crimb(50));
//        System.out.println(crimbImproved(50));
        System.out.println(climbStairsWithCache(50));
    }

    public static Long crimb(long n){
        if(n == 1) {
            return 1L;
        }else if (n==2){
            return 2L;
        }else{
            return crimb(n-1) + crimb(n-2);
        }
    }


    public static Long crimbImproved(int n){
        Long[] arr = new Long[n+1];
        if(n==1){
            arr[1] = 1L;
        }else if(n==2){
            arr[2] = 2L;
        }else{
            arr[1] = 1L;
            arr[2] = 2L;
            for(int i =3;i<n+1;i++) {
                arr[i] = arr[i - 1] + arr[i - 2];
            }
        }
        return arr[n];
    }

    private static HashMap<Integer,Long> cache = new HashMap<>();
    public static Long climbStairsWithCache(int n) {
        if(!cache.containsKey(n)){
            cache.put(n,help(n));
        }
        return cache.get(n);
    }

    public static Long help(int n){
        if(n==1)
            return 1L;
        else if (n==2)
            return 2L;
        else
            return climbStairsWithCache(n-1) + climbStairsWithCache(n-2);
    }
}
