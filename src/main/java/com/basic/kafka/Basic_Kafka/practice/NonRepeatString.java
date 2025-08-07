package com.basic.kafka.Basic_Kafka.practice;

import java.util.ArrayList;
import java.util.List;

class NonRepeatString {
    public static int lengthOfLongestSubstring(String s) {
        String arr[] = s.split("");
        if(s.equals("")) {
            return 0;
        }
        int maxLength = 0;
        List<String> nonRepeatedString = new ArrayList<>();
        for(int i = 0; i < arr.length; i ++) {
            if(nonRepeatedString.contains(arr[i])) {
                int index = nonRepeatedString.indexOf(arr[i]);
                List<String> nonRep = new ArrayList<>();
                nonRep.addAll(nonRepeatedString.subList(index+1, nonRepeatedString.size()));
                nonRepeatedString.clear();
                nonRepeatedString.addAll(nonRep);
                nonRepeatedString.add(arr[i]);
            } else {
                nonRepeatedString.add(arr[i]);
            }
            maxLength = Math.max(maxLength, nonRepeatedString.size());

        }

        return maxLength;
    }
    public static void main(String[] args) {
        String str = "abcabcbb";
        System.out.print(lengthOfLongestSubstring(str));
    }
}