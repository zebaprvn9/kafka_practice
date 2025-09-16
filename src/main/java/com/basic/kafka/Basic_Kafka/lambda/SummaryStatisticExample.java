package com.basic.kafka.Basic_Kafka.lambda;

import java.util.IntSummaryStatistics;
import java.util.stream.IntStream;

public class SummaryStatisticExample {
    public static void main(String[] args) {
        int[] arr = {24, 34, 52, 12, 8, 2};

        //from above IntStream example we have to call IntStream with each we want to call avg, min or max
        // we can avoid calling each time with help of summary statistic.
        IntSummaryStatistics summaryStatistics = IntStream.of(arr).summaryStatistics();
        System.out.println("max from array " + summaryStatistics.getMax());
        System.out.println("min from array " + summaryStatistics.getMin());
        System.out.println("avg from array " + summaryStatistics.getAverage());
        System.out.println("count from array " + summaryStatistics.getCount());
        System.out.println("sum from array " + summaryStatistics.getSum());
    }
}
