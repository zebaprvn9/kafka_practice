package com.basic.kafka.Basic_Kafka.thread;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

public class ExecuteMultipleTaskReturnAny {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(3);
        List<Callable<String>> tasks = Arrays.asList(
                () -> {Thread.sleep(1000); return "First Task";},
                () -> {Thread.sleep(2000); return  "Second Task";},
                () -> {return "Immediately completed";}
        );

       String firstTask =  executorService.invokeAny(tasks);
       System.out.println(firstTask);
    }
}
