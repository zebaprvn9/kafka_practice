package com.basic.kafka.Basic_Kafka.practice;


import java.util.*;

class TimeMap {

    static class TimeStampValue implements Comparable<TimeStampValue> {

        String value;
        Integer timeStamp;

        TimeStampValue(Integer timeStamp, String value) {
            this.timeStamp = timeStamp;
            this.value = value;
        }

        @Override
        public boolean equals(Object obj) {
            Integer intobj = (Integer) obj;

            return timeStamp.equals(intobj);
        }

        @Override
        public int hashCode() {
            return timeStamp.hashCode();
        }

        @Override
        public int compareTo(TimeStampValue o) {
            return this.timeStamp.compareTo(o.timeStamp);
        }
    }

    Map<String, Set<TimeStampValue>> keyValuePair = null;

    TimeMap() {
        keyValuePair = new HashMap<>();
    }

    public void set(String key, String value, Integer timeStamp) {
        List<String> str = new ArrayList<>();

        Set<TimeStampValue> timeStampValues = this.keyValuePair.get(key);
        if(timeStampValues == null) {
            timeStampValues = new TreeSet<>(Comparator.reverseOrder());
            timeStampValues.add(new TimeStampValue(timeStamp, value));
        } else {
            timeStampValues.add(new TimeStampValue(timeStamp, value));
        }
        keyValuePair.put(key, timeStampValues);
    }

    public String get(String key, Integer timeStamp) {
        Set<TimeStampValue> timeStampValues = keyValuePair.get(key);
        if(timeStampValues == null) {
            return "";
        }
        for(TimeStampValue timeStampValue : timeStampValues) {
            if(timeStampValue.timeStamp == timeStamp) {
                return timeStampValue.value;
            } else if (timeStampValue.timeStamp < timeStamp) {
                return timeStampValue.value;
            }
        }
        return "";
    }

    public static void main(String[] args) {

        TimeMap timeMap = new TimeMap();
        timeMap.set("foo", "bar", 1);
        System.out.println(timeMap.get("foo", 1));
        System.out.println(timeMap.get("foo", 3));
        timeMap.set("foo", "bar2", 4);
        System.out.println(timeMap.get("foo", 4));
        System.out.println(timeMap.get("foo", 5));

    }



}
