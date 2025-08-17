package com.basic.kafka.Basic_Kafka.thread;

public class JoinVsThread {

    public static void main(String[] args) throws InterruptedException {
        Thread worker = new Thread(()->{
            System.out.println("worker started .....");
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {

            }
            System.out.println("worker thread finished");
        });

        worker.start();

        // pausing main thread till worker thread complete its task.
        worker.join();
        System.out.println("Main thread thread continues after worker thread");
        //pause main thread for 1 min
        Thread.sleep(1000);
        System.out.println("Main thread resume after sleep for 1 second");

    }
}
