package com.basic.kafka.Basic_Kafka.rate_limiter;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class RateLimiter {
    private final int maxRequests;
    private final long timeWindowMillis;
    private final Map<String, Deque<Long>> userRequests;

    public RateLimiter(int maxRequests, int timeWindowSeconds) {
        this.maxRequests = maxRequests;
        this.timeWindowMillis = timeWindowSeconds * 1000L;
        this.userRequests = new ConcurrentHashMap<>();
    }

    public boolean isAllowed(String userId) {
        long now = System.currentTimeMillis();

        // Each user has its own queue of request timestamps
        userRequests.putIfAbsent(userId, new ArrayDeque<>());

        Deque<Long> requests = userRequests.get(userId);

        // Synchronize only on this user's queue
        synchronized (requests) {
            // Remove expired timestamps
            while (!requests.isEmpty() && (now - requests.peekFirst()) >= timeWindowMillis) {
                System.out.println("removing expired request");
                requests.pollFirst();
            }

            if (requests.size() < maxRequests) {
                requests.addLast(now);
                return true;
            } else {
                return false;
            }
        }
    }

    // Demo
    public static void main(String[] args) throws InterruptedException {
        RateLimiter rateLimiter = new RateLimiter(5, 60); // 5 requests per 60 sec

        String userA = "userA";
        String userB = "userB";

        // User A makes 5 requests
        System.out.println("---- User A ----");
        for (int i = 0; i < 5; i++) {
            System.out.println("UserA request " + (i + 1) + ": " + rateLimiter.isAllowed(userA));
        }

        // User B makes 4 requests
        System.out.println("\n---- User B ----");
        for (int i = 0; i < 4; i++) {
            System.out.println("UserB request " + (i + 1) + ": " + rateLimiter.isAllowed(userB));
        }

        // User A tries a 6th request (blocked)
        System.out.println("\nUserA extra request: " + rateLimiter.isAllowed(userA));

        // User B still allowed (since only 4 so far)
        System.out.println("UserB 5th request: " + rateLimiter.isAllowed(userB));

        // Wait for 60 seconds to reset UserA's window
        System.out.println("\nSleeping for 60 seconds...");
        Thread.sleep(60_000);

        // User A allowed again after window expires
        System.out.println("\nUserA request after waiting: " + rateLimiter.isAllowed(userA));
    }
}
