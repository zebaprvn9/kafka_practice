package com.basic.kafka.Basic_Kafka.practice;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


/**
 * Given an array of distinct integers candidates and a target integer target, return a list of all unique combinations of candidates where the chosen numbers sum to target. You may return the combinations in any order.
 *
 * The same number may be chosen from candidates an unlimited number of times. Two combinations are unique if the frequency of at least one of the chosen numbers is different.
 *
 * The test cases are generated such that the number of unique combinations that sum up to target is less than 150 combinations for the given input.
 *
 *
 *
 * Example 1:
 *
 * Input: candidates = [2,3,6,7], target = 7
 * Output: [[2,2,3],[7]]
 * Explanation:
 * 2 and 3 are candidates, and 2 + 2 + 3 = 7. Note that 2 can be used multiple times.
 * 7 is a candidate, and 7 = 7.
 * These are the only two combinations.
 * Example 2:
 *
 * Input: candidates = [2,3,5], target = 8
 * Output: [[2,2,2,2],[2,3,3],[3,5]]
 * Example 3:
 *
 * Input: candidates = [2], target = 1
 * Output: []
 */
class CombinationSum {
    public static List<List<Integer>> combinationSum(int[] candidates, int target) {
        List<List<Integer>> result = new ArrayList<>();
        for(int i = 0; i < candidates.length; i++) {
            int candidate = candidates[i];
            if(target % candidates[i] == 0) {
                int num = target / candidates[i];
                List<Integer> subList = new ArrayList<>();
                while(num > 0) {
                    subList.add(candidates[i]);
                    num--;
                }
                result.add(subList);
            }
            Integer[] subArray = Arrays.stream(candidates)
                    .boxed()
                    .toArray(Integer[]::new);

            List<Integer> remainingList = Arrays.asList(subArray);
            int k = 0;
            int l = 1;
            int num = candidate;
            while(num > 0) {
                k++;
                int combinationValue = target - (l*candidate);
                if(remainingList.contains(combinationValue)) {
                    List<Integer> subList = new ArrayList<>();
                    while(k > 0) {
                        subList.add(candidate);
                        k--;
                    }
                    subList.add(combinationValue);
                    int sum = subList.stream()
                            .mapToInt(Integer::intValue)
                            .sum();
                    if(sum == target) {
                        result.add(subList);
                    }
                }
                num = target - (l*num);
                l++;
            }
        }
        return result;
    }

    public static void main(String[] args) {
        int arr[] = {2,3,5};
        int target  = 8;
        System.out.print(combinationSum(arr, target));

    }
}