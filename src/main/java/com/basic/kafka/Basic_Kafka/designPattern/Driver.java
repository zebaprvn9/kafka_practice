package com.basic.kafka.Basic_Kafka.designPattern;

/**
 * File: model/Driver.java
 */

public class Driver {
    public final String driverId;
    public volatile double lat;
    public volatile double lon;
    public volatile double rating;     // optional: for strategy variants
    public volatile double etaSeconds; // optional: precomputed ETA to hotspot

    public Driver(String driverId, double lat, double lon, double rating) {
        this.driverId = driverId;
        this.lat = lat;
        this.lon = lon;
        this.rating = rating;
    }
}
