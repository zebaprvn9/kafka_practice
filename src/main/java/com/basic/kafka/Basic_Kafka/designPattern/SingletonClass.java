package com.basic.kafka.Basic_Kafka.designPattern;

public class SingletonClass {

    private static volatile SingletonClass instance;

    private SingletonClass() {
        if(instance != null) {
            throw new RuntimeException("Use getInstant Method to get instance");
        }
    }

    public static SingletonClass getInstance() {
        if(instance == null) {
            instance = new SingletonClass();
        }
        return instance;
    }

    public void showMessage() {
        System.out.println("Hello from singleton class");
    }


    public static void main(String[] args) {
        SingletonClass s1 = SingletonClass.getInstance();
        SingletonClass s2 = SingletonClass.getInstance();
        System.out.println(s1 == s2);

        s1.showMessage();
    }
}
