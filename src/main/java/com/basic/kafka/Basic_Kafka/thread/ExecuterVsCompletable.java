package com.basic.kafka.Basic_Kafka.thread;
import java.util.*;
import java.util.concurrent.*;


public class ExecuterVsCompletable {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<Integer> future = executorService.submit(()->{
            //Thread.sleep(1000);
            return 5;
        });
        Integer result = future.get();

        //we are blocked at .get() funciton and we cant perform anything until get is finished

        CompletableFuture<Integer> completableFuture = CompletableFuture.supplyAsync(()->{
            return 5;
        }).thenApply(amount->{
            int discount = amount - 1;
            return discount;
        }).thenApply(finalAmount -> {
            int taxed = finalAmount +2;
            return taxed;
        }).exceptionally( ex -> {
            return  0;
        });

        int finalAmount = completableFuture.get();
    }
}
