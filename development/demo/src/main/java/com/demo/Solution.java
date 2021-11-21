package com.demo;

import java.util.LinkedHashMap;

/**
 * @author kevin
 * @date 2021/11/2
 * @desc
 */
public class Solution {
    public static void main(String[] args) {
        System.out.println(search("gbgkkdehhl", 2, 1));
    }

    /**
     *
     * @param str 输入字符串
     * @param m   出现同一次数中的次序
     * @param n   出现次数
     * @return
     */
    public static Character search(String str,int m,int n){
        LinkedHashMap<Character, Integer> dict = new LinkedHashMap<>();
        for (char ch : str.toCharArray()) {
            dict.put(ch,dict.getOrDefault(ch,0) + 1);
        }
        for (char ch : dict.keySet()) {
            if(dict.get(ch) == n){
                if(--m == 0){
                    return ch;
                }
            }
        }
        return null;
    }
}
