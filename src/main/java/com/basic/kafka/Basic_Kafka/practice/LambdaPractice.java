package com.basic.kafka.Basic_Kafka.practice;

import lombok.extern.slf4j.Slf4j;

import java.io.FilterOutputStream;
import java.time.LocalDate;
import java.time.Period;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class LambdaPractice {


    public static void main(String[] args) {

        //stream API
        List<String> names = Arrays.asList("alice", "bob", "charlie");
        List<Integer> numbers = Arrays.asList(1,2,3,4,5,6);

        List<String> result = numbers.stream()
                .filter(num->num%2==0)
                .map(num->num*2)
                .map(String::valueOf)
                .collect(Collectors.toList());

        result.forEach(log::info);
        names.forEach(log::info);

        String duplicate = "swiss";
        log.info(String.valueOf(firstNonDuplicateChar(duplicate)));

        Period age = fetchPeriodDate(1990, 6, 1);
        log.info("years : {}, months: {}, days: {}", age.getYears(), age.getMonths(), age.getDays());

        //use remove if
        List<String> newName = new ArrayList<>(names);
        newName.removeIf(name-> name.startsWith("c"));
        newName.forEach(log::info);
        log.info(newName.stream().collect(Collectors.joining(",")));
        log.info(String.join(",", newName));

        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6);

        int sum = list.stream().collect(Collectors.summingInt(num-> num));
        int sum2 = list.stream().mapToInt(num-> num).sum();
        log.info(String.valueOf(sum));
        log.info("via map: {} ",sum2);

        List<String> listt = Arrays.asList("apple", "banana", "cherry");

        List<String> upperCaseList =  listt.stream().map(String::toUpperCase).collect(Collectors.toList());
        upperCaseList.forEach(log::info);

        List<String> listr = Arrays.asList("Apple", "Banana", "avocado", "cherry");

        List<String> resultList  = listr.stream().filter(element-> element.startsWith("A") || element.startsWith("a")).collect(Collectors.toList());
        resultList.forEach(log::info);

        String input = "engineering";
        LinkedHashMap<Character, Long> resultMap = input.chars().mapToObj(c->(char)c)
                .collect(Collectors.groupingBy(Function.identity(), LinkedHashMap::new, Collectors.counting()));

        Character charText =  resultMap.entrySet()
                .stream().filter(entry-> entry.getValue()>1)
                .map(Map.Entry::getKey).findFirst().orElse(null);

        log.info(String.valueOf(charText));


        List<String> listByLength = Arrays.asList("one", "three", "five", "six", "seven");

        Map<Integer, List<String>> listByLengthResult =  listByLength.stream().collect(Collectors.groupingBy(String::length));
        listByLengthResult.forEach((length, word)->{
            log.info("Length:{}, word:{}", length, word);
        });

        List<Integer> numList = Arrays.asList(5, 3, 9, 1, 7);

        IntSummaryStatistics intSummaryStatistics = numList.stream().mapToInt(num-> num).summaryStatistics();
        log.info("min:{}, max:{}", intSummaryStatistics.getMin(), intSummaryStatistics.getMax());

        List<Integer> nums = Arrays.asList(1, 2, 3, 2, 4, 5, 1);


        LinkedHashMap<Integer, Long> mapData =  nums.stream().collect(Collectors.groupingBy(Function.identity(), LinkedHashMap::new, Collectors.counting()));
        Set<Integer> duplicates = mapData.entrySet().stream()
                .filter(entry->entry.getValue()>1).map(Map.Entry::getKey).collect(Collectors.toSet());
        log.info("duplicates:{}", duplicates);

        List<String> words = Arrays.asList("apple", "banana", "kiwi", "strawberry", "fig");

        Optional<String> longestString  = words.stream().max(Comparator.comparingInt(String::length));
        log.info("longestString: {}", longestString.get());

        List<String> words1 = Arrays.asList("apple", "banana", "kiwi", "strawberry", "plu","fig", "one");

        Optional<String> shortestLength = words1.stream().min(Comparator.comparingInt(String::length));
        log.info("shortestLength:{} ", shortestLength);


        List<String> words2 = Arrays.asList("apple", "banana", "kiwi", "strawberry", "fig");

        Long longVal = words2.stream().filter(w-> w.length()>5).count();
        log.info("string:{} ", longVal);

        List<String> wordsJoin = Arrays.asList("apple", "banana", "kiwi");

        log.info(String.join(",", wordsJoin) );

        List<String> wordsSort = Arrays.asList("banana", "apple", "avocado", "cherry");
        List<String> sortedWord = wordsSort.stream().filter(str-> str.startsWith("a")|| str.startsWith("A")).sorted().collect(Collectors.toList());
        log.info("sorted str:{}", sortedWord);

        List<String> wordsDuplicate = Arrays.asList("apple", "banana", "apple", "kiwi", "banana");
        wordsDuplicate.stream().distinct().forEach(log::info);

        Map<String, Integer> sortMap = new HashMap<>();
        sortMap.put("apple", 3);
        sortMap.put("banana",5);
        sortMap.put("kiwi", 2);
        sortMap.entrySet().stream().sorted(Map.Entry.<String, Integer>comparingByValue().reversed() )
                .forEach(entry->System.out.print("key:"+entry.getKey()+"value:"+entry.getValue()));



    }

    public static Character firstNonDuplicateChar(String str) {

        Map<Character, Long> charCountMap = str.chars().mapToObj(c->(char)c)
                .collect(Collectors.groupingBy(Function.identity(), LinkedHashMap::new, Collectors.counting()));

        return charCountMap.entrySet().stream().filter(entry-> entry.getValue()==1).map(Map.Entry::getKey).findFirst().orElse(null);
    }

    public static Period fetchPeriodDate(int year, int month, int day) {
        LocalDate currentTime = LocalDate.now();
        return Period.between(LocalDate.of(year, month, day), currentTime);
    }
}
