package com.basic.kafka.Basic_Kafka.practice;

import java.util.HashMap;
import java.util.Map;

public class MinimumWindowSubString {

    public static String minWindow(String s, String t) {

        if(s.length() < t.length()) {
            return "";
        }

        Map<Character, Integer> need = new HashMap<>();
        for(char c : t.toCharArray()) {
            need.put(c, need.getOrDefault(c,0)+1);
        }

        Map<Character, Integer> window = new HashMap<>();
        int have = 0; int needCount = need.size();
        int left = 0; int minLen = Integer.MAX_VALUE; int start = 0;
        for(int right = 0; right < s.length() ; right++) {
            char c = s.charAt(right);
            window.put(c, window.getOrDefault(c, 0)+1);
            if(need.containsKey(c) && need.get(c).intValue() == window.get(c).intValue()) {
                have++;
            }
            while(have == needCount) {
                if((right-left+1)<minLen) {
                    minLen = right-left+1;
                    start = left;
                }
                char charLeft = s.charAt(left);
                window.put(charLeft, window.get(charLeft)-1);
                if(need.containsKey(charLeft) && window.get(charLeft) < need.get(charLeft)) {
                    have--;
                }
                left++;
            }
        }

        return minLen == Integer.MAX_VALUE ? "" : s.substring(start, start+  minLen);
    }

    public static void main(String[] args) {
        System.out.print(minWindow("ADOBECODEBANC", "ABC"));
    }
}
