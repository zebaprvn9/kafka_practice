package com.basic.kafka.Basic_Kafka.practice;

import java.util.*;

class ThreeSum {
    public static List<List<Integer>> threeSum(int[] nums) {
        List<List<Integer>> result = new ArrayList<>();
        List<String> temp = new ArrayList<>();
        for(int i =0; i < nums.length; i++) {

            for(int j=i+1; j< nums.length;j++){

                for(int k =j+1; k<nums.length;k++) {

                    int[] triplet = new int[]{nums[i], nums[j], nums[k]};
                    Arrays.sort(triplet); // Sort to handle duplicates

                    String tempText = triplet[0] + "," + triplet[1] + "," + triplet[2]; // Use comma-separated for safety

                    if((nums[i]+ nums[j]+nums[k] )==0 && !temp.contains(tempText)) {
                        temp.add(tempText);
                        List<Integer> subList = new ArrayList<>();
                        subList.add(nums[i]);
                        subList.add(nums[j]);
                        subList.add(nums[k]);
                        result.add(subList);
                    }
                }
            }
        }
        return result;
    }

    public static void main(String[] args) {
        int arr [] =  {-1,0,1,2,-1,-4};
        System.out.print(threeSum(arr));
    }
}