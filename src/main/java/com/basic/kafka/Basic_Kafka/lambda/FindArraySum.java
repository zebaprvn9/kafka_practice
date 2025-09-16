package com.basic.kafka.Basic_Kafka.lambda;


import java.util.stream.IntStream;

public class FindArraySum {

    public static void main(String[] args) {
        int[] arr = {24, 34, 52, 12, 8, 2};
        //if we do normal way we have to iterate across each element in array and find average
        // this can be replaced with stream java 8 library as follows
        int sum = IntStream.of(arr).sum();
        System.out.println(sum);
    }
}
