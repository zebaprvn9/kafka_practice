package com.basic.kafka.Basic_Kafka.practice;

import java.util.ArrayList;
import java.util.List;

public class SubSetGeneration {

    public static List<List<Integer>> generateSubSet(int arr[]) {
        List<List<Integer>> result = new ArrayList<>();
        backTrack(arr, 0, new ArrayList<>(), result);
        return result;
    }

    private static void backTrack(int arr[], int index, List<Integer> subList, List<List<Integer>> result) {

        result.add(new ArrayList<>(subList));
        for (int i = index; i < arr.length; i++) {
            subList.add(arr[i]);
            backTrack(arr, i+1, subList, result);
            subList.remove(subList.size()-1);
        }
    }

    public static void main(String[] args) {
        int arr[] = {1,2};
        System.out.print(generateSubSet(arr));
    }
}
