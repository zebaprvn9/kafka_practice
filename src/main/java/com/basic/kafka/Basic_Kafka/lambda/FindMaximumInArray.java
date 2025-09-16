package com.basic.kafka.Basic_Kafka.lambda;


import java.util.stream.IntStream;

public class FindMaximumInArray {

    public static void main(String[] args) {
        int[] arr = {24, 34, 52, 12, 8, 2};
        //if we do normal way we have to iterate across each element in array and find maximum
        // this can be replaced with stream java 8 library as follows
        IntStream.of(arr).max().ifPresent(System.out::print);
    }
}
