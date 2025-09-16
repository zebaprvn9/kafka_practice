package com.basic.kafka.Basic_Kafka.thread;

import java.util.Arrays;
import java.util.List;

public class StreamVsParelellStream {

    public static void main(String[] args) {
        List<String> streamList = Arrays.asList("a","b","c","d");
        streamList.parallelStream().forEachOrdered(System.out::print);
        System.out.println();
        streamList.stream().forEach(System.out::print);
    }
}
