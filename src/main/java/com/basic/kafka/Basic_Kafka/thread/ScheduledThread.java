package com.basic.kafka.Basic_Kafka.thread;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ScheduledThread {

    public static void main(String[] args) {
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        Runnable runnable = () -> {
            System.out.println("Scheduled Thread for 2 second");
        };
        scheduledExecutorService.schedule(runnable, 2, TimeUnit.SECONDS);
    }
}
