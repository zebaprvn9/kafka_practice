package com.basic.kafka.Basic_Kafka.practice;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class InsertInterval {
    public static int[][] insert(int[][] intervals, int[] newInterval) {

        List<int[]> result = new ArrayList<>();

        // add all element which is smaller than new interval.
        int i = 0;
        int n = intervals.length;
        while(i < n && intervals[i][1] < newInterval[0]) {
            result.add(intervals[i]);
            i++;
        }

        //merge intervals with similar range
        while(i < n && intervals[i][0] <= newInterval[1]) {
            newInterval[0] = Math.min(intervals[i][0], newInterval[0]);
            newInterval[1] = Math.max(intervals[i][1], newInterval[1]);
            i++;
        }
        result.add(newInterval);
        //add rest of intervals
        while (i < n) {
            result.add(intervals[i]);
            i++;
        }
        return result.toArray(new int[result.size()][]);
    }

    public static void main(String[] args) {
        int[][] arr = new int[][] {
                {1, 2},
                {3, 5},
                {6, 7},
                {8, 10},
                {12, 16}
        };
        int newInterval[] = {2,5};
        int [][] str = insert(arr, newInterval);
        for (int i = 0; i < str.length; i++) {
            for (int j = 0; j < str[i].length; j++) {
                System.out.print(str[i][j] + " ");
            }
            System.out.println(); // Move to next line after each row
        }
    }
}