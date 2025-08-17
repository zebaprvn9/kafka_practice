package com.basic.kafka.Basic_Kafka.practice;

import java.util.*;

class KClosest {
    public static int[][] kClosest(int[][] points, int k) {
        List<int[]> pointList = new ArrayList<>(Arrays.asList(points));
        pointList.sort(Comparator.comparingDouble(p->Math.sqrt(p[0]*p[0] + p[1]*p[1])));
        return pointList.subList(0, k).toArray(new int[k][2]);
    }

    public static void main(String[] args) {
        int[][] arr = {
            {1,0},
            {0,1}
        };
        int[][] closestPoints = kClosest(arr, 2);

        // Print output
        for (int[] point : closestPoints) {
            System.out.println(Arrays.toString(point));
        }
    }
}