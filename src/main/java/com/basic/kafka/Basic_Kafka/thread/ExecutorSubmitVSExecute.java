package com.basic.kafka.Basic_Kafka.thread;

import java.util.concurrent.*;

public class ExecutorSubmitVSExecute {

    public static void main(String[] args) {
        ExecutorService executorService = Executors.newFixedThreadPool(2);

        Runnable runnableWorker = () -> {
            System.out.println("runnable is working using executer execute");
        };

        //execute method does not return any value as future and only take runnable as input
        //no result, exception is visible
        for(int i = 0; i < 3; i++) {
            executorService.execute(runnableWorker);
        }

        ExecutorService executorServiceSubmit = Executors.newFixedThreadPool(3);

        //submit method does  return value as future and take runnable/callable as input
        //no result, exception is visible
        Callable<?> callableWorker = () -> {
            System.out.println("callable is running Using executor submit");
          return 5;
        };
        Future<?> future = executorServiceSubmit.submit(callableWorker);
        try {
            System.out.println(future.get());
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
        executorServiceSubmit.shutdown();
        executorService.shutdown();
    }
}
