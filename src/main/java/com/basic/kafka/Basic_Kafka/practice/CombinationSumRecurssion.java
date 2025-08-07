package com.basic.kafka.Basic_Kafka.practice;

import java.util.ArrayList;
import java.util.List;

public class CombinationSumRecurssion {


    public static List<List<Integer>> combinationSum(int[] candidates, int target) {
        List<List<Integer>> result = new ArrayList<>();
        backtrack(candidates, 0, target, new ArrayList<>(), result);
        return result;
    }

    private static void backtrack(int[] candidates, int index, int target, List<Integer> current, List<List<Integer>> result) {
        if (target == 0) {
            result.add(new ArrayList<>(current)); // Found a valid combination
            return;
        }
        if (target < 0) {
            return; // Exceeded target
        }

        for (int i = index; i < candidates.length; i++) {
            current.add(candidates[i]); // Choose the number
            backtrack(candidates, i, target - candidates[i], current, result); // Not i+1 because we can reuse same number
            current.remove(current.size() - 1); // Backtrack
        }
    }

    public static void main(String[] args) {
        int arr[] = {2,3,5};
        int target  = 8;
        System.out.print(combinationSum(arr, target));

    }
}
