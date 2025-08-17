package com.basic.kafka.Basic_Kafka.practice;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

class MedianFinder {

    PriorityQueue<Integer> small;
    PriorityQueue<Integer> large;
    public MedianFinder() {
        small = new PriorityQueue<>(Comparator.reverseOrder());
        large = new PriorityQueue<>();
    }
    
    public void addNum(int num) {
        small.add(num);
        large.add(small.poll());
        if(small.size() < large.size()) {
            small.add(large.poll());
        }
    }
    
    public double findMedian() {
        if(small.size() > large.size()) {
            return small.peek();
        } else {
            return (small.peek() + large.peek())/2.0;
        }
    }

    public static void main(String[] args) {
        MedianFinder medianFinder = new MedianFinder();
        medianFinder.addNum(5);
        System.out.println(medianFinder.findMedian());
        medianFinder.addNum(10);
        System.out.println(medianFinder.findMedian());
        medianFinder.addNum(15);
        System.out.println(medianFinder.findMedian());
        medianFinder.addNum(20);
        System.out.println(medianFinder.findMedian());

    }
}

/**
 * Your MedianFinder object will be instantiated and called as such:
 * MedianFinder obj = new MedianFinder();
 * obj.addNum(num);
 * double param_2 = obj.findMedian();
 */