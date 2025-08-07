package com.basic.kafka.Basic_Kafka.practice;

import java.util.Arrays;

public class ProductOfArrayExceptSelf {

    public static int[] productExceptSelf(int[] nums) {

        int length = nums.length;

        int[] prefix = new int[length];
        prefix[0] = 1;
        int[] suffix = new int[length];
        suffix[length-1] = 1;
        // prepare prefix
        for(int i = 1; i < length; i++) {
            prefix[i] = prefix[i-1] * nums[i-1];
        }
        //prepare suffix
        for(int i = length-2; i >= 0; i--) {
                suffix[i] = suffix[i+1] * nums[i+1];
        }
        for(int i = 0; i < length; i++) {
            nums[i] = prefix[i] * suffix[i];
        }
        return nums;
     }

    public static void main(String[] args) {
        int arr[] = {1,2,3,4};
        System.out.print(Arrays.toString(productExceptSelf(arr)));
    }
}
