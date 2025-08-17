package com.basic.kafka.Basic_Kafka.thread;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ThreadVsExecuterService {
    public static void main(String[] args) {

        Runnable task = () -> {
            System.out.println(Thread.currentThread().getName() + " is running");
        };

        Thread t1 = new Thread(task, "Ayzal");
        Thread t2 = new Thread(task, "Ayan");
        t1.start();
        t2.start();

        ExecutorService executorService = Executors.newFixedThreadPool(3);

        Runnable worker = () -> {
            System.out.println(Thread.currentThread().getName() + " is running");
        };

        for (int i = 0; i < 5; i++) {
            executorService.submit(worker);
        }

        executorService.shutdown();

    }



}
