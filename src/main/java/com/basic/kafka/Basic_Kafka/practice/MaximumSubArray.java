package com.basic.kafka.Basic_Kafka.practice;

class MaximumSubArray {
    public static int maxSubArray(int[] nums) {
        int maxSum = nums[0];
        int currentSum = nums[0];
        for(int i = 1; i < nums.length; i ++) {
            currentSum = Math.max(nums[i],  currentSum + (nums[i]));
            maxSum = Math.max(maxSum, currentSum);
        }
        return maxSum;
    }

    public static void main(String[] args) {
        int arr [] = {4, -1, 2, 1, -10, 5};
        System.out.println(maxSubArray(arr));
    }
}