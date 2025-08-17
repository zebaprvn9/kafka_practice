package com.basic.kafka.Basic_Kafka.thread;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ProducerConsumerUsingThread {

    public static void main(String[] args) {
        BlockingQueue<Integer> queue = new ArrayBlockingQueue<>(5);

        ExecutorService service = Executors.newFixedThreadPool(2);

        Runnable producer = () -> {
            try {
                for(int i = 1; i <= 10; i++) {
                    queue.put(i);
                    System.out.println("Produces: " + i);
                    Thread.sleep(1000);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        };

        Runnable consumer = () -> {
            try {
                while(true) {
                    Integer value = queue.take();
                    System.out.println("Consumed: " + value);
                    Thread.sleep(1000);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        };

        service.submit(producer);
        service.submit(consumer);

        service.shutdown();

    }
}
