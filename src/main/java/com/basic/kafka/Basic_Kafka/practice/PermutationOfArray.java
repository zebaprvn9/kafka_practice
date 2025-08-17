package com.basic.kafka.Basic_Kafka.practice;

import java.util.ArrayList;
import java.util.List;

public class PermutationOfArray {

    public static List<List<Integer>> arrayPermutation(int arr[]) {

        List<List<Integer>> result = new ArrayList<>();
        List<Integer> subList = new ArrayList<>();
        boolean []used = new boolean[arr.length];
        backTrack(arr, used, subList, result);
        return  result;
    }

    private static void backTrack(int arr[], boolean[] used, List<Integer> subList, List<List<Integer>> result) {

        if(subList.size() == arr.length) {
            result.add(new ArrayList<>(subList));
        }
        for(int i = 0; i < arr.length; i ++) {
            if(used[i]) {
                continue;
            }
            used[i] = true;
            subList.add(arr[i]);
            backTrack(arr, used, subList, result);
            used[i] = false;
            subList.remove(subList.size() - 1);
        }
    }

    public static void main(String[] args) {

        int arr[] = {1,2,3};
        System.out.print(arrayPermutation(arr));

    }
}