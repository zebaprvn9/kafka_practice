package com.basic.kafka.Basic_Kafka.practice;

import java.util.Arrays;
import java.util.IntSummaryStatistics;
import java.util.Objects;

public class MinStack {

    Integer[] stack = null;
    int top = 0;
    public MinStack() {
        this.stack = new Integer[20];
        this.top = 0;
    }
    
    public void push(int val) {
        if(stack.length == top) {
            Integer[] temp = new Integer[(top*2)];
            System.arraycopy(stack, 0, temp, 0, top);
            temp[top+1] = val;
            stack = temp;
        } else {
            stack[top] = val;
        }
        top = top + 1;
    }
    
    public void pop() {
        if(top != 0) {
            this.stack[top-1] = null;
            top = top - 1;
        }
    }
    
    public int top() {
        return this.stack[top - 1];
    }
    
    public int getMin() {
        IntSummaryStatistics intSummaryStatistics = Arrays.stream(this.stack).filter(Objects::nonNull).mapToInt(num->num).summaryStatistics();
        return intSummaryStatistics.getMin();
    }

    public static void main(String[] args) {
        MinStack minStack = new MinStack();
        minStack.push(2);
        minStack.push(-1);
        minStack.push(0);
        minStack.push(-2);
        minStack.pop();
        minStack.push(-3);
        minStack.pop();
        minStack.pop();
        minStack.push(-6);
        System.out.print(minStack.top());
        System.out.print(Arrays.toString(minStack.stack));
        System.out.print(minStack.getMin());
    }
}