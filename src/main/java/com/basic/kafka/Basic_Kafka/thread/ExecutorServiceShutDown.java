package com.basic.kafka.Basic_Kafka.thread;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ExecutorServiceShutDown {
    public static void main(String[] args) throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        Runnable runnable = () -> {
            System.out.println("executing thread via executorService");
        };
        for (int i = 0; i < 3; i ++) {
            Thread.sleep(5000);
        }
        for (int i = 0; i < 3; i ++) {
            executorService.submit(runnable);
        }
        Thread.sleep(4000);
        System.out.println("shutting down executor");
        executorService.shutdown();

        try {
            if(!executorService.awaitTermination(2, TimeUnit.SECONDS)) {
                System.out.println("shutting down executor as it is still not shut down");
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            System.out.println("Force closing now as some exception came");
            executorService.shutdownNow();
            throw new RuntimeException(e);
        }
    }
}
