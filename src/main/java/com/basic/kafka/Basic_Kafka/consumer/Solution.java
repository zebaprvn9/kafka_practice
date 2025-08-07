package com.basic.kafka.Basic_Kafka.consumer;

import java.util.*;

class Solution {
    public List<List<Integer>> threeSum(int[] nums) {
        List<List<Integer>> result = new ArrayList<>();
        Map<String, Integer> temp = new HashMap<>();
        for(int i =0; i < nums.length; i++) {

            for(int j=i+1; j< nums.length;j++){

                for(int k =j+1; k<nums.length;k++) {

                    if((nums[i]+ nums[j]+nums[k] )==0) {
                        List<Integer> subList = new ArrayList<>();
                        subList.add(nums[i]);
                        subList.add(nums[j]);
                        subList.add(nums[k]);

                        result.add(subList);
                        Collections.sort(subList);
                        temp.put(subList.toString(), 1);
                    }
                }
            }
        }
        return null;
    }

    public static void main(String[] args) {
        List<Integer> tt = new ArrayList<>();
        tt.add(1);
        tt.add(0);
        tt.add(-1);
        List<Integer> ll = new ArrayList<>();
        ll.add(0);
        ll.add(1);
        ll.add(-1);
        Map<String, Integer> temp = new HashMap<>();
        Collections.sort(tt);
        Collections.sort(ll);
        System.out.println(tt);
        System.out.println(ll);
        temp.put(tt.toString(), 1);
        if(!tt.toString().equals(ll.toString())) {
            temp.put(ll.toString(), 1);
        }
        List<Integer> mm = new ArrayList<>();
        mm.add(2);
        mm.add(0);
        mm.add(-2);
        if(!tt.toString().equals(mm.toString())) {
            temp.put(mm.toString(), 1);
        }
        System.out.println(temp);

    }
}