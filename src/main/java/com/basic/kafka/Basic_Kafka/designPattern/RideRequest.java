package com.basic.kafka.Basic_Kafka.designPattern;

// File: model/RideRequest.java
public class RideRequest {
    public final String rideId;
    public final String riderId;
    public final double lat;
    public final double lon;
    public final long requestedAt;

    public RideRequest(String rideId, String riderId, double lat, double lon, long requestedAt) {
        this.rideId = rideId;
        this.riderId = riderId;
        this.lat = lat;
        this.lon = lon;
        this.requestedAt = requestedAt;
    }
}
