package com.basic.kafka.Basic_Kafka.core_java;

class A {
    int x = 10;
    public A() {
        System.out.println("A called");
        show();
    }
    void show() {
        System.out.println("A: " + x);
    }
}
class B extends A {
    int y = 20;

    void show() {
        System.out.println("B: " + y);
    }
}
public class Test {
    public static void main(String[] args) {

        new B();
    }
}