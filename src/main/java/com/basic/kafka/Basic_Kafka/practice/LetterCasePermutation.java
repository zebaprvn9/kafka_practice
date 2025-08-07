package com.basic.kafka.Basic_Kafka.practice;

import java.util.ArrayList;
import java.util.List;

public class LetterCasePermutation {

    public static List<String> letterPermutation(String str) {

        List<String> result = new ArrayList<>();
        StringBuilder permutatedText = new StringBuilder();
        backTrack(str, 0, permutatedText, result);
        return result;
    }

    public static void backTrack(String mainString, int index, StringBuilder permutatedText, List<String> result) {
        if(index == mainString.length()) {
            result.add(permutatedText.toString());
            return;
        }
        char c = mainString.charAt(index);
        if(Character.isLetter(c)) {

            //convert to lower case
            permutatedText.append(Character.toLowerCase(c));
            backTrack(mainString, index + 1, permutatedText, result);
            permutatedText.deleteCharAt(permutatedText.length() - 1);

            //convert to uppercase
            permutatedText.append(Character.toUpperCase(c));
            backTrack(mainString, index + 1, permutatedText, result);
            permutatedText.deleteCharAt(permutatedText.length() - 1);
        } else {

            //add numbers
            permutatedText.append(c);
            backTrack(mainString, index + 1, permutatedText, result);
            permutatedText.deleteCharAt(permutatedText.length() - 1);
        }
    }

    public static void main(String[] args) {
        String str = "a1b";
        System.out.print(letterPermutation(str));
    }

}
