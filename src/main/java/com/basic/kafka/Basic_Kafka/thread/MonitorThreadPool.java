package com.basic.kafka.Basic_Kafka.thread;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class MonitorThreadPool {

    public static void main(String[] args) throws InterruptedException {
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(2,
                4,
                10,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>());

        for(int i = 1; i <= 10; i++) {
            final int taskId = i;
            threadPoolExecutor.execute(() -> {
                try {
                    System.out.println("Task ID: " + taskId + " for Thread " + Thread.currentThread().getName());
                    Thread.sleep(2000);
                } catch (InterruptedException e) {

                }
            });
        }

        if(!threadPoolExecutor.isTerminated()) {
            System.out.println("Active Thread: " + threadPoolExecutor.getActiveCount()
            + "Completed Task" + threadPoolExecutor.getCompletedTaskCount()
            + "Queue size: " + threadPoolExecutor.getQueue().size());
            Thread.sleep(1000);
            if(threadPoolExecutor.getCompletedTaskCount() == 10) {
                threadPoolExecutor.shutdown();
            }
        }

    }
}
