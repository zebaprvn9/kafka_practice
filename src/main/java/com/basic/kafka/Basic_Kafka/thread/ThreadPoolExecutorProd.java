package com.basic.kafka.Basic_Kafka.thread;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPoolExecutorProd {

    /**
     * we should not use Executor Service with thread pool as it uses a
     * nonblocking queue which can cuse OOM error so in prod
     * we should avoid Executors factory class create thread pool instead we should ThreadPoolExecutor.
     */
    public static void main(String[] args) {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                2,
                4,
                10,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(2),
                Executors.defaultThreadFactory(),
                new ThreadPoolExecutor.AbortPolicy()
        );

        for (int i = 0; i < 3; i ++) {

            final int taskId = i;
            executor.execute(() -> {
                System.out.println("task: + " + taskId + "Thread : " + Thread.currentThread().getName());
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        executor.shutdown();

        /**
         * RejectedExecutionHandler gets throw when abort policy is called via ThreadPoolExecutor.AbortPolicy
         *
         */

        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(2,
                4,
                10,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(2),
                new ThreadPoolExecutor.CallerRunsPolicy()
                );
        for(int i = 0; i < 3; i ++) {
            final int taskId = i;
            threadPoolExecutor.execute(() -> {
                System.out.println("Task ID : " + taskId + " using Thread + " + Thread.currentThread().getName());
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        threadPoolExecutor.shutdown();
    }
}
