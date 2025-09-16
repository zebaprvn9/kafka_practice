package com.basic.kafka.Basic_Kafka.lambda;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class FunctionalInterface implements Runnable {

    @Override
    public void run() {
        System.out.println("this is my run method");
    }

    public static void main(String[] args) {
        //old way of starting thread
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(new FunctionalInterface());

        //anonymous inner class example 1
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                System.out.println("using anonymous inner class");
            }
        });
        //anonymous inner class example 2, passing functionality as argument. we can do it with help of inner class.
        Runnable calculateTax = new Runnable() {
            @Override
            public void run() {
                //calculate tax
            }
        };

        executorService.submit(calculateTax);

        // using lambda -> conceptually lambda are shorthand for some special type of anonymous inner class
        //shorthand are of types -> single abstract method or single method interface or functional interfaces


        Runnable calculateTaxUsingLambda = () -> {
          System.out.println("calculating tax");
        };

        executorService.submit(calculateTaxUsingLambda);
    }

}
