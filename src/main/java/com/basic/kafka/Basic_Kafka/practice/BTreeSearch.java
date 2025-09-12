package com.basic.kafka.Basic_Kafka.practice;

import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

class BTreeSearch {
    public static int search(int[] nums, int target) {

        int mid = nums.length / 2;
        int length = nums.length;
        int k = mid;

        if(length <= 1 && nums[0] == target) {
            return 0;
        }
        for(int i = 0; i <= mid; i++) {
            if(nums[i] == target) {
                return i;
            }
            if(k < length) {
                if (nums[k] == target) {
                    return k;
                } else {
                    k++;
                }
            }
        }
        return -1;
    }

    public static void main(String[] args) {
        int arr[] = {1,3,0};
        Set<String> s = new TreeSet<>();

        System.out.print(search(arr, 0));
    }
}