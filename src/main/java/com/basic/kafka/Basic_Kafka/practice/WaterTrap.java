package com.basic.kafka.Basic_Kafka.practice;

class WaterTrap {
    public static int trap(int[] height) {
        int trapWater = 0;
        int length = height.length;
        int k  = 1;
        for(int i = 0; i < height.length; i++) {
            if(k == height.length - 1) {
                if(height[length-1] < height[length-2]) {
                    continue;
                } else {
                    trapWater = trapWater + Math.min(height[i], height[i-1]);
                }
            } else {
                trapWater = trapWater + Math.min(height[i], height[i+1]);
            }
            k++;
        }
        return trapWater;
    }

    public static void main(String[] args) {
        int arr[] = {4,2,0,3,2,5};
        System.out.print(trap(arr));

    }
}