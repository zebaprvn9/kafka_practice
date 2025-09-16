package com.basic.kafka.Basic_Kafka.lambda;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Distinct3MinimumFromArray {

    public static void main(String[] args) {

        //return distinct minimum element from array
        int[] arr = {4, 3, 89, 23, 2, 0, 2};
        int[] copyArr = Arrays.copyOf(arr, arr.length);
        Arrays.sort(copyArr);
        int[] finalArr = Arrays.copyOf(copyArr, 3);
        System.out.println(Arrays.toString(finalArr));
        //with int stream
        IntStream.of(arr).distinct().sorted()
                .limit(3)
                .forEach(System.out::println);

        // Stream work as
        //create stream -> process Stream -> consume stream
        // we create stream in above example using IntStream then we process stream using distinct/sorted/process
        // then we consume stream using sum or foreach.
        IntStream.range(0, 10).forEach(System.out::print);
        int[] array = IntStream.range(0, 99).toArray();
        System.out.print(Arrays.toString(array));
        List<Integer> list = IntStream.range(0, 23).boxed().collect(Collectors.toList());
        list.forEach(System.out::print);
        System.out.println(IntStream.of(arr).anyMatch(ele -> ele % 2 == 1));
        System.out.println(IntStream.of(arr).allMatch(ele-> ele % 2 == 1));
    }
}
