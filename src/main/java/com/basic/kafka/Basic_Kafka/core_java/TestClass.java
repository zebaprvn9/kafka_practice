package com.basic.kafka.Basic_Kafka.core_java;


import org.apache.kafka.common.metrics.stats.Rate;

import java.time.LocalDate;
import java.util.concurrent.*;

class RateLimiter {
    long noOfRequest;

    public RateLimiter(long noOfRequest, TimeUnit timeDuration, int seconds
    ) {
        this.noOfRequest = noOfRequest;
        this.timeDuration = timeDuration;
        this.seconds = seconds;
    }

    TimeUnit timeDuration;
    int seconds;



    public static RateLimiter getInstance() {
        return new RateLimiter(5, TimeUnit.SECONDS, 60);
    }
}

public class TestClass {

    public static void main(String[] args) {

        RateLimiter rateLimiter = RateLimiter.getInstance();
        //CountDownLatch countDownLatch = new CountDownLatch(rateLimiter.noOfRequest);


    }

}
