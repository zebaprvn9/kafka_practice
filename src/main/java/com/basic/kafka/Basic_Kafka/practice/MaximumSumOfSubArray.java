package com.basic.kafka.Basic_Kafka.practice;

public class MaximumSumOfSubArray {

    public static int maximumSumSubArray(int arr[], int k) {

        int maxSum = 0;
        for(int i = 0; i < arr.length; i ++) {
            int window = 0;
            int sum = 0;
            while (window < k && (window + i) < arr.length - 1) {
                sum = sum + arr[window+i];
                window++;
            }
            maxSum = Math.max(sum, maxSum);
        }
        return maxSum;
    }

    public static void main(String[] args) {
        int arr[] = {1, 4, 2, 10, 23, 3, 1, 0, 20};
        System.out.print(maximumSumSubArray(arr, 4));
    }
}
