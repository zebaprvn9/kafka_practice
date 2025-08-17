package com.basic.kafka.Basic_Kafka.practice;

import java.util.*;
import java.util.stream.Collectors;

public class GenerateBalanceParenthesis {

    public static List<String> generateBalanceParenthesis(int n) {
        List<String> result = new ArrayList<>();
        Stack<String> open = new Stack<>();
        Stack<String> close = new Stack<>();
        int k = 0, j = 0;
        while(k < n) {
            open.push("(");
            k++;
        }
        while(j < n) {
            open.push(")");
            j++;
        }
        backTrack(n, 0, 0, "", result);
        return result;
    }

    public static void backTrack(int n,int open, int close, String pattern, List<String> result) {

        if(pattern.length() == n*2) {
            result.add(pattern);
        }
        if(open < n) {
            backTrack(n, open + 1, close, pattern + "(", result);
        }
        if(close < open) {
            backTrack(n, open, close + 1, pattern + ")", result);
        }
    }

    public static void main(String[] args) {
        //System.out.print(generateBalanceParenthesis(2));
        Map<String, Integer> map = new HashMap<>();
        map.put("1", 1);
        map.put("4", 4);
        map.put("2", 2);
        map.forEach((s, integer) -> System.out.println(s + " " + integer));
        map.entrySet().stream().sorted(Map.Entry.comparingByValue()).forEach(
                entry->{System.out.println(entry.getKey() + " " + entry.getValue());}
        );
    }
}
