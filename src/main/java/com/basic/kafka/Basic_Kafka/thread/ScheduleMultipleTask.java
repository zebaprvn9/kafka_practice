package com.basic.kafka.Basic_Kafka.thread;


import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

public class ScheduleMultipleTask {

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(3);
        List<Callable<String>> tasks = Arrays.asList(
                () -> {Thread.sleep(1000); return "Task one is executed";},
                () -> {Thread.sleep(2000); return "Task two is executed";},
                () -> {Thread.sleep(3000); return "Task three is executed";}
        );

        List<Future<String>> results = executorService.invokeAll(tasks);

        for(Future<String> result : results) {
            System.out.println(result.get());
        }

        executorService.shutdown();
    }
}
